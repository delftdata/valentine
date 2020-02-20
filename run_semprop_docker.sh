#!/bin/bash

set -x

docker run -i -v `pwd`:/code/  \
		-w /code/ \
		--entrypoint python  \
		-e "PYTHONPATH=/code/"	\
		asteriosk/sempropenv:3.8.5 \
		run_job.py -c $1
