#!/bin/bash

set -x

# temp_file=$(mktemp /tmp/semprop-docker.XXXXXX) || { echo "Failed to create temp file"; exit 1; }

docker run -a STDIN -a STDOUT -a STDERR -v `pwd`:/code/  \
		-w /code/ \
#		--cidfile $temp_file		
		--entrypoint python  \
		-e "PYTHONPATH=/code/"	\
		asteriosk/sempropenv:3.8.5 \
		run_job.py -c $1

# docker container wait `cat $temp_file`

rm -f $temp_file
