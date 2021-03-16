import click
from lithops import FunctionExecutor, Storage

# Used inside lambda functions
import io
from shutil import copyfileobj
from lithops.storage.cloud_proxy import open
import numpy as np

max_num_categories = 256 # max value of a byte

def radix_sort_by_first_byte(key_name, bucket_name, input_prefix, bytes_to_classify):
	partition_id = key_name[len(input_prefix)+1:]
	available_num_categories = int(max_num_categories / bytes_to_classify)

	partition_sink = io.BytesIO()
	with open(key_name, 'rb') as source_file:
		# Maybe change to using very large buffers to reduce python overhead
		# or read all data into memory, classify and then start writing
		# Classification of skewed data might lead to very large and empty buffers
		# Sort first, then traverse the list
		copyfileobj(source_file, partition_sink)

	partition_buffer = partition_sink.getbuffer()
	record_arr = np.frombuffer(partition_buffer, dtype=np.dtype([('first', 'u1'), ('rest', 'V99')]))
	sorted_partition = np.sort(record_arr, order='first')
	categorized_partition = sorted_partition['first'] // bytes_to_classify

	is_unique = np.empty(categorized_partition.shape, dtype=np.bool_)
	is_unique[:1] = True
	is_unique[1:] = categorized_partition[1:] != categorized_partition[:-1]
	category_start_indices = np.where(is_unique)[0]
	# take into account that slicing is exclusive of last element
	category_end_indices = np.append(category_start_indices[1:], len(categorized_partition))

	for i in range(len(category_start_indices)):
		start_index = category_start_indices[i]
		end_index = category_end_indices[i]

		# Need to do this and can't directly use i
		# Not all categories might be represented in this partition
		category_id = categorized_partition[start_index]
		with open(f'{input_prefix}-intermediate/{category_id}/{partition_id}', 'wb') as category_file:
			category_file.write(memoryview(sorted_partition[start_index:end_index]))

	# while True:
	# 	# each record is 100 byte
	# 	# the read and write are buffered, so such small reads and writes should be fine
	# 	record = source_file.read(100) 
	# 	if not record:
	# 		break

	# 	# Classify using first byte
	# 	category_id = int(record[0] / bytes_to_classify)
	# 	available_categories[category_id].write(record)

	return True


def sort_category(category_prefix, bucket_name, output_prefix):
	storage_client = Storage()
	category_id = category_prefix.split('/')[-1]
	key_list = storage_client.list_keys(bucket_name, category_prefix + '/')

	category_sink = io.BytesIO()
	for key_name in key_list:
		with open(key_name, 'rb') as source_file:
			copyfileobj(source_file, category_sink)

	category_buffer = category_sink.getbuffer()
	record_arr = np.frombuffer(category_buffer, dtype=np.dtype([('key', 'V10'), ('value', 'V90')]))
	sorted_category = np.sort(record_arr, order='key')

	with open(f'{output_prefix}/{category_id}', 'wb') as sorted_file:
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
			extra_args=[bucket, input_prefix, bytes_to_classify], include_modules=None)
		results = fexec.get_result(fs=radix_sort_futures)
		# print(results)

		sort_category_futures = fexec.map(sort_category, intermediate_categories,
			extra_args=[bucket, output_prefix], include_modules=None)
		results = fexec.get_result(fs=sort_category_futures)
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
