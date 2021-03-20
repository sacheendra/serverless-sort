import click
from lithops import FunctionExecutor, Storage

# Used inside lambda functions
import io
import gc
import math
from util import copyfileobj
from smart_open import open
import numpy as np

record_size = 100 # bytes
max_num_categories = 256 # max value of a byte
buffer_size_to_categorize = 500 * (10 ** 6) # Approx 0.5GB

# Takes input size in GB
def make_plan(input_size):
	num_shuffles = 0
	current_category_size = input_size
	while current_category_size > buffer_size_to_categorize:
		num_shuffles = num_shuffles + 1
		# Each shuffle creates upto 256 categories
		# A byte of the key can be used to categorize the records
		# This creates categories of records which ordered. Their contents are not
		# cat 1 < 2 < 3 ...
		current_category_size = current_category_size // 256
	
	# For the last shuffle, all 256 categories might not be needed
	# The easiest way is halve the number of categories. Halving categories doubles the category size
	# Double the category size and see if it still fits in the buffer
	values_per_category = 1 # Each value gets its own unique category to start with
	while (current_category_size * 2) < buffer_size_to_categorize:
		current_category_size = current_category_size * 2
		values_per_category = values_per_category * 2
	
	return (num_shuffles, values_per_category)

	

def radix_sort_by_byte(keys_list, prefix, category_stack, values_per_category, storage, id):
	partition_id = id # Just a unique name for this partition. The actual value doesn't matter
	available_num_categories = int(max_num_categories / values_per_category)
	index_of_byte_to_sort = len(category_stack)
	return_keys_list = []

	data_buf = memoryview(bytearray(buffer_size_to_categorize))
	unique_buf = np.empty(buffer_size_to_categorize // record_size, dtype=np.bool_)
	iteration_id = 0

	def sort_buffer(buffer_to_sort):
		nonlocal iteration_id
		nonlocal return_keys_list

		is_unique = unique_buf[:len(buffer_to_sort) // record_size]

		record_arr = np.frombuffer(buffer_to_sort, dtype=np.dtype([('first', 'u1'), ('rest', 'V99')]))
		sorted_buffer = np.sort(record_arr, order='first')
		categorized_buffer = sorted_buffer['first'] // values_per_category

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
			partition_file_key = f'{prefix}/{str(category_stack)}/{category_id}/{partition_id}/iter{iteration_id}'
			partition_file_name = f's3://{storage.bucket}/{partition_file_key}'
			with open(partition_file_name, 'wb',
				transport_params=dict(client=storage.get_client(), multipart=False)) as partition_file:

				partition_file.write(memoryview(sorted_buffer[start_index:end_index]))
				return_keys_list.append(partition_file_key)

		iteration_id = iteration_id + 1

	buffer_start = 0
	for key_name in keys_list:
		with open(f's3://{storage.bucket}/{key_name}', 'rb',
			transport_params=dict(client=storage.get_client())) as source_file:

			# Store data into leftover buffer space
			while (bytes_read := source_file.readinto(data_buf[buffer_start:])) != 0:
				buffer_start = buffer_start + bytes_read
				# Sort buffer when it is full
				if buffer_start == buffer_size_to_categorize:
					sort_buffer(data_buf)
					buffer_start = 0
					# Read same file again

	# Sort any data leftover in buffer
	# We end at buffer_start because that is the start of the next write
	# We want to sort already written data
	sort_buffer(data_buf[:buffer_start])

	return {
		'keys_list': return_keys_list,
		'category_stack': category_stack
	}


def sort_category(keys_list, prefix, category_stack, consider_last_byte_sorted, storage, id):
	storage_client = Storage()
	category_id = id
	num_bytes_already_sorted = len(category_stack)
	if not consider_last_byte_sorted:
		num_bytes_already_sorted = num_bytes_already_sorted - 1
	num_bytes_to_sort = 10 - num_bytes_already_sorted

	category_sink = io.BytesIO()
	for key_name in keys_list:
		with open(f's3://{storage.bucket}/{key_name}', 'rb',
			transport_params=dict(client=storage.get_client())) as source_file:

			copyfileobj(source_file, category_sink)

	category_buffer = category_sink.getbuffer()
	record_arr = np.frombuffer(category_buffer,
		dtype=np.dtype([('sorted', f'V{num_bytes_already_sorted}'), ('key', f'V{num_bytes_to_sort}'), ('value', 'V90')]))
	sorted_category = np.sort(record_arr, order='key')

	with open(f's3://{storage.bucket}/{prefix}/{category_id}', 'wb',
		transport_params=dict(client=storage.get_client())) as sorted_file:

		sorted_file.write(memoryview(sorted_category))

	return True

@click.command()
@click.option('--input-prefix', type=str, default='10g-100mb-input', help='Prefix used for input data inside the bucket')
@click.option('--output-prefix', type=str, default='10g-100mb-output', help='Prefix to use for output data inside the bucket')
@click.option('--max-parallelism', type=int, default=None, help='Maximum number of concurrent workers')
@click.option('--image', type=str, default='sacheendra/lithops-sort-1', help='Docker image to use')
def sort_command(input_prefix, output_prefix, max_parallelism, image):
	storage_client = Storage()
	bucket = None
	input_info_lis = None
	
	with FunctionExecutor(runtime=image, workers=max_parallelism) as fexec:
		bucket = fexec.config['lithops']['storage_bucket']
		input_info_list = storage_client.list_objects(bucket, input_prefix + '/')
		input_size = sum(info['Size'] for info in input_info_list)
		(num_shuffles, last_values_per_category) = make_plan(input_size)
		
		current_values_per_category = 1
		current_prefix = input_prefix
		current_keys_list = [ {
			'keys_list': [key_name],
			'prefix': input_prefix + '-intermediate0',
			'category_stack': []
		} for key_name in storage_client.list_keys(bucket, input_prefix + '/') ]
		for current_shuffle in range(num_shuffles):
			# Change values per category of last shuffle
			if current_shuffle == num_shuffles-1:
				current_values_per_category = last_values_per_category
			
			radix_sort_futures = fexec.map(radix_sort_by_byte, current_keys_list,
				extra_args={'values_per_category':current_values_per_category}, include_modules=['util'])
			radix_sort_results = fexec.get_result(fs=radix_sort_futures)
			
			categories_keys_lists = {}
			for res in radix_sort_results:
				intermediate_keys_list = res['keys_list']
				input_category_stack = res['category_stack']
				for key_name in intermediate_keys_list:
					category_id = int(key_name.rsplit(sep='/', maxsplit=3)[-3])
					new_category_stack = input_category_stack + [category_id]
					new_category_stack_str = '/'.join([str(x) for x in new_category_stack])
					if new_category_stack_str in categories_keys_lists:
						categories_keys_lists[new_category_stack_str].append(key_name)
					else:
						categories_keys_lists[new_category_stack_str] = [key_name]
					
			# Partition category lists
			# Attach prefix metadata so that sorter knows what to name files
			each_category_size = input_size / ((256 / current_values_per_category) * (current_shuffle + 1))
			num_partitions_per_category = math.ceil(each_category_size / buffer_size_to_categorize)
			
			current_keys_list = []
			for category_stack_str, cat_keys_list in categories_keys_lists.items():
				for sub_list in np.array_split(cat_keys_list, num_partitions_per_category):
					partition_entry = {
						'keys_list': sub_list,
						'prefix': f'{input_prefix}-intermediate{str(current_shuffle + 1)}',
						'category_stack': [int(x) for x in category_stack_str.split('/')]
					}
					current_keys_list.append(partition_entry)
			
		consider_last_byte_sorted = False
		if last_values_per_category == 1:
			consider_last_byte_sorted = True
		for entry in current_keys_list:
			entry['prefix'] = output_prefix
		sorted_keys_list = sorted(current_keys_list, key=lambda x: x['category_stack'])
		sort_category_futures = fexec.map(sort_category, sorted_keys_list,
			extra_args={'consider_last_byte_sorted':consider_last_byte_sorted}, include_modules=['util'])
		results = fexec.get_result(fs=sort_category_futures)
		# print(results)

	# Check if size of output matches size of input
	
	output_info_list = storage_client.list_objects(bucket, output_prefix)
	output_size = sum(info['Size'] for info in output_info_list)
	assert input_size == output_size, f'input size: {input_size}, output_size: {output_size}'

	print('Done!')



if __name__ == '__main__':
	sort_command()

