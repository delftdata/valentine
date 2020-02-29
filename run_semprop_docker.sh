#!/bin/bash

set -x

docker run --rm -i -v `pwd`:/code_readonly/ -w /code_readonly/ --entrypoint=/bin/bash --env="PYTHONPATH=/code/" asteriosk/sempropenv:3.8.5 -c "./entrypoint.sh \"$1\""
