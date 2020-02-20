#!/bin/bash

echo "$1"

docker run -i -v `pwd`:/code/  -w /code/ --entrypoint /bin/bash  asteriosk/sempropenv:3.8.5 -c "$1"
