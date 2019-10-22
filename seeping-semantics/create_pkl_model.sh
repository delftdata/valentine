#!/bin/bash

path="$(dirname "$0")"
cd $path/aurum-datadiscovery

echo $'\n'Create model in pickle format $'\n'
python networkbuildercoordinator.py --opath test/testmodel/