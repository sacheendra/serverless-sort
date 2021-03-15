## Serverless Sort using Lithops

This application generates, sorts, and validates data as per http://sortbenchmark.org/.

It uses the [Lithops](https://github.com/lithops-cloud/lithops) framework.

It has only been tested with AWS Lambda.

### Setup

Build the included Dockerfile and upload to the appropriate cloud using the instructions at https://github.com/lithops-cloud/lithops/tree/master/runtime.

For AWS Lambda: `lithops runtime build -f MyDockerfile docker_username/my_container_runtime -b aws_lambda`.

Add the required credentials to `sample_lithops_config` and rename it to `.lithops_config`. Change any other fields you want to.

### Run

You can generate data using `python generate_data.py generate --image docker_username/my_container_runtime`. RUn `python generate_data.py generate --help` to see other options.

Sort the data using `python sort_data.py`

Validate that the data has been correctly sorted using `python generate_data.py validate`.