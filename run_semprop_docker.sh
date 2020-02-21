#!/bin/bash

set -x

# temp_file=$(mktemp semprop-docker.XXXXXX) || { echo "Failed to create temp file"; exit 1; }

# rm -f $temp_file

docker run --rm -i -v `pwd`:/code/  \
		-w /code/ \
		--entrypoint=python  \
		--env="PYTHONPATH=/code/" \
		asteriosk/sempropenv:3.8.5 \
		run_job.py -c "$1"

# docker container wait `cat $temp_file`

# rm -f $temp_file