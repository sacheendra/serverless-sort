import subprocess
import os
from retry import retry

num_retries = 5
num_records_per_gigabyte = 10000000

@retry(tries=num_retries, backoff=5)
def generate_data(num_partitions):
	generate_cmd = ['python', 'generate_data.py', 'generate',
		'--number', str(num_records_per_gigabyte), # 100 bytes * 1e7 = 1gb
		'--prefix', f'320g-1g-input',
		'--partitions', str(num_partitions)
	]
	subprocess.run(generate_cmd, check=True)

@retry(tries=num_retries, backoff=5)
def sort_data(max_parallelism, iteration=0):
	os.environ['__LITHOPS_SESSION_ID'] = f'320g-1g-{max_parallelism}workers-sort-iter{iteration}'
	sort_cmd = ['python', 'sort_data.py',
		'--input-prefix', f'320g-1g-input',
		'--output-prefix', f'320g-1g-output',
		'--max-parallelism', str(max_parallelism),
		'--bytes-to-classify', '1'
	]
	subprocess.run(sort_cmd, env=os.environ, check=True)

@retry(tries=num_retries, backoff=5)
def validate_data():
	validate_cmd = ['python', 'generate_data.py', 'validate',
		'--prefix', '320g-1g-output'
	]
	completed = subprocess.run(validate_cmd, stdout=subprocess.PIPE, check=True)
	output_text = completed.stdout.decode('utf-8')
	if 'Success!' not in output_text:
		raise Exception('Validation failed:\n' + output_text)


def main():
	max_parallelism_values = [10, 20, 40, 80, 160, 320]
	# max_parallelism_values = [10]

	for current_max_parallelism in max_parallelism_values:
		print(f'Started run with parameter: parallelism: {current_max_parallelism}')

		# Generate data every experiment to avoid caching effects
		generate_data(max(max_parallelism_values))


		# Run each experiment twice
		# Once to warm up, to reduce cold start times
		# Another time for actual measurement
		for iteration in range(2):
			sort_data(current_max_parallelism, iteration)


		validate_data()

		print(f'Completed run with parameter: parallelism: {current_max_parallelism}')


if __name__ == '__main__':
	main()