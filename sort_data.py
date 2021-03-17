import click
from lithops import FunctionExecutor, Storage

# Used inside lambda functions
import io
import gc
from util import copyfileobj
from smart_open import open
import numpy as np

record_size = 100 # bytes
max_num_categories = 256 # max value of a byte
buffer_size_to_categorize = 500 * (10 ** 6) # Approx 0.5GB

def radix_sort_by_first_byte(key_name, bucket_name, input_prefix, bytes_to_classify, storage):
	partition_id = key_name[len(input_prefix)+1:]
	available_num_categories = int(max_num_categories / bytes_to_classify)
	category_files = [  for category_id in range(available_num_categories) ]

	buf = memoryview(bytearray(buffer_size_to_categorize))
	unique_buf = np.empty(buffer_size_to_categorize // record_size, dtype=np.bool_)
	source_file = open(f's3://{storage.bucket}/{key_name}', 'rb',
		transport_params=dict(client=storage.get_client()))

	iteration_id = 0
	while (bytes_read := source_file.readinto(buf)) != 0:
		buffer_to_sort = buf[:bytes_read]
		is_unique = unique_buf[:bytes_read // record_size]

		record_arr = np.frombuffer(buffer_to_sort, dtype=np.dtype([('first', 'u1'), ('rest', 'V99')]))
		sorted_buffer = np.sort(record_arr, order='first')
		categorized_buffer = sorted_buffer['first'] // bytes_to_classify
	
		is_unique[:1] = True
		is_unique[1:] = categorized_buffer[1:] != categorized_buffer[:-1]
		category_start_indices = np.where(is_unique)[0]
		# take into account that slicing is exclusive of last element
		category_end_indices = np.append(category_start_indices[1:], len(categorized_buffer))
	
		for i in range(len(category_start_indices)):
			start_index = category_start_indices[i]
			end_index = category_end_indices[i]
	
			# Need to do this and can't directly use i
			# Not all categories might be represented in this partition
			category_id = categorized_buffer[start_index]
			with open(f's3://{storage.bucket}/{input_prefix}-intermediate/{category_id}/{partition_id}/iter{iteration_id}', 'wb',
				transport_params=dict(client=storage.get_client(), multipart=False)) as category_file:

				category_file.write(memoryview(sorted_buffer[start_index:end_index]))

		iteration_id = iteration_id + 1

	source_file.close()

	return True


def sort_category(category_prefix, bucket_name, output_prefix, storage):
	storage_client = Storage()
	category_id = category_prefix.split('/')[-1]
	key_list = storage_client.list_keys(bucket_name, category_prefix + '/')

	category_sink = io.BytesIO()
	for key_name in key_list:
		with open(f's3://{storage.bucket}/{key_name}', 'rb',
			transport_params=dict(client=storage.get_client())) as source_file:

			copyfileobj(source_file, category_sink)

	category_buffer = category_sink.getbuffer()
	record_arr = np.frombuffer(category_buffer, dtype=np.dtype([('key', 'V10'), ('value', 'V90')]))
	sorted_category = np.sort(record_arr, order='key')

	with open(f's3://{storage.bucket}/{output_prefix}/{category_id}', 'wb',
		transport_params=dict(client=storage.get_client())) as sorted_file:

		sorted_file.write(memoryview(sorted_category))

	return True

@click.command()
@click.option('--input-prefix', type=str, default='10g-100mb-input', help='Prefix used for input data inside the bucket')
@click.option('--output-prefix', type=str, default='10g-100mb-output', help='Prefix to use for output data inside the bucket')
@click.option('--bytes-to-classify', type=int, default=2, help='Number of bytes to use for classification in radix sort')
@click.option('--max-parallelism', type=int, default=None, help='Maximum number of concurrent workers')
@click.option('--image', type=str, default='sacheendra/lithops-sort-1', help='Docker image to use')
def sort_command(input_prefix, output_prefix, bytes_to_classify, max_parallelism, image):
	available_num_categories = int(max_num_categories / bytes_to_classify)
	intermediate_prefix = f'{input_prefix}-intermediate'
	intermediate_categories = [f'{intermediate_prefix}/{i}' for i in range(available_num_categories)]

	storage_client = Storage()
	bucket = None

	with FunctionExecutor(runtime=image, workers=max_parallelism) as fexec:
		bucket = fexec.config['lithops']['storage_bucket']
		keys_list = storage_client.list_keys(bucket, input_prefix + '/')

		radix_sort_futures = fexec.map(radix_sort_by_first_byte, keys_list,
			extra_args=[bucket, input_prefix, bytes_to_classify], include_modules=['util'])
		results = fexec.get_result(fs=radix_sort_futures)
		# print(results)

		# sort_category_futures = fexec.map(sort_category, intermediate_categories,
		# 	extra_args=[bucket, output_prefix], include_modules=['util'])
		# results = fexec.get_result(fs=sort_category_futures)
		# print(results)

	# Check if size of output matches size of input
	input_info_list = storage_client.list_objects(bucket, input_prefix + '/')
	input_size = sum(info['Size'] for info in input_info_list)
	output_info_list = storage_client.list_objects(bucket, output_prefix + '/')
	output_size = sum(info['Size'] for info in output_info_list)
	assert input_size == output_size, f'input size: {input_size}, output_size: {output_size}'

	print('Done!')



if __name__ == '__main__':
	sort_command()
