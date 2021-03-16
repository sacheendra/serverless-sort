import click
from lithops import FunctionExecutor, Storage

# Used inside lambda functions
import io
import subprocess
from shutil import copyfileobj
from smart_open import open

record_size = 100
summary_postfix = '-summaries'

def generate_records(partition_id, num_records, key_prefix, storage):
	
	with open(f's3://{storage.bucket}/{key_prefix}/{partition_id}', 'wb',
		transport_params=dict(client=storage.get_client())) as dest_file:

		cmd = ['./gensort', f'-b{partition_id * num_records}', str(num_records), '/dev/stdout']
		with subprocess.Popen(cmd, stdout=subprocess.PIPE) as p:
			with p.stdout as genoutput:
				copyfileobj(genoutput, dest_file)
			returncode = p.wait()
			if returncode != 0:
				raise Exception(f'Non-zero return code for gensort: {returncode}')

	return True

def validate_records(key_name, bucket, key_prefix, storage):
	returncode = 0
	stderr_output = None

	with open(f's3://{storage.bucket}/{key_name}', 'rb',
		transport_params=dict(client=storage.get_client())) as source_file:

		cmd = ['./valsort', '-o', '/dev/stdout', '/dev/stdin'] # Keep the -q option in mind in case output pollutes summary
		with subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE) as p:
			with p.stdout as valoutput, p.stderr as valerr:
				with p.stdin as valinput: # Need to close input for valsort to finish
					copyfileobj(source_file, valinput)
				returncode = p.wait()
				if returncode != 0:
					stderr_output = valerr.read().decode('utf-8')
				if returncode > 1:
					raise Exception(f'Non-zero return code for valsort: {returncode}\n' + stderr_output)

				partition_id = key_name[len(key_prefix)+1:]
				with open(f's3://{storage.bucket}/{key_prefix}{summary_postfix}/{partition_id}', 'wb',
					transport_params=dict(client=storage.get_client())) as summary_file:

					copyfileobj(valoutput, summary_file)

	if returncode == 0:
		return {
			'success': True,
			'stderr': None
		}
	elif returncode == 1:
		return {
			'success': False,
			'stderr': stderr_output
		}

def validate_summaries(key_prefix, bucket_name, storage):
	storage_client = Storage()
	key_list = storage_client.list_keys(bucket_name, key_prefix + '/')
	sorted_key_list = sorted(key_list, key=lambda x: int(x.split('/')[-1]))

	summaries_buf = io.BytesIO()

	# Get all summaries into one buffer
	for key_name in sorted_key_list:
		with open(f's3://{storage.bucket}/{key_name}', 'rb',
			transport_params=dict(client=storage.get_client())) as source_file:

			copyfileobj(source_file, summaries_buf)

	cmd = ['./valsort', '-s', '/dev/stdin']
	with subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE) as p:
		with p.stdin as valinput, p.stdout as valoutput, p.stderr as valerr:
			with p.stdin as valinput: # Need to close input for valsort to finish
				valinput.write(summaries_buf.getbuffer())
			returncode = p.wait()
			if returncode != 0:
				raise Exception(f'Non-zero return code for valsort: {returncode}\n' + valerr.read().decode('utf-8'))
			valoutput_str = valoutput.read().decode('utf-8')

			return valoutput_str



@click.group()
def cli():
	pass

@cli.command('generate')
@click.option('--number', type=int, default=1000000, help='Number of records per partition; default is 10^6')
@click.option('--prefix', type=str, default='10g-100mb-input', help='Prefix to use for input data inside the bucket')
@click.option('--partitions', type=int, default=100, help='Number of input partitions')
@click.option('--image', type=str, default='sacheendra/lithops-sort-1', help='Docker image to use')
def generate_command(number, prefix, partitions, image):
	bucket = None
	with FunctionExecutor(runtime=image) as fexec:
		bucket = fexec.config['lithops']['storage_bucket']
		futures = fexec.map(generate_records, range(partitions),
			extra_args=[number, prefix], include_modules=['util'])
		results = fexec.get_result(fs=futures)
		print(results)

	partition_size = record_size * number

	# Check if all files have been uploaded
	storage_client = Storage()
	partition_list = storage_client.list_objects(bucket, prefix + '/')
	assert len(partition_list) == partitions, f'partition_list: {len(partition_list)}; partitions: {partitions}'
	for info in partition_list:
		assert info['Size'] == partition_size, f'partition size: {partition_size} \ninfo: {info}'

	print('Done!')

@cli.command('validate')
@click.option('--prefix', type=str, default='10g-100mb-output', help='Prefix used for sorted files')
@click.option('--image', type=str, default='sacheendra/lithops-sort-1', help='Docker image to use')
def validate_command(prefix, image):
	storage_client = Storage()

	with FunctionExecutor(runtime=image) as fexec:
		bucket = fexec.config['lithops']['storage_bucket']
		key_list = storage_client.list_keys(bucket, prefix + '/')

		validate_records_futures = fexec.map(validate_records, key_list,
			extra_args=[bucket, prefix], include_modules=['util'])
		results = fexec.get_result(fs=validate_records_futures)
		for index, r in enumerate(results):
			if not r['success']:
				print(f'Failed to validate partition: {key_list[index]}')
				print(r['stderr'])
				return

		validate_summaries_futures = fexec.map(validate_summaries, [prefix+summary_postfix],
			extra_args=[bucket], include_modules=['util'])
		results = fexec.get_result(fs=validate_summaries_futures)
		if results[0] == '':
			print('Success!')
		else:
			print(results)



if __name__ == '__main__':
	cli()
