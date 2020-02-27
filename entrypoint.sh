#!/bin/bash

set -x

echo "$1"

apt-get install -y rsync

TARGET="/code/"
SOURCE="/code_readonly/"

OUTPUT="$SOURCE""output"
DATA="$SOURCE""data"

#Copy the contents of this folder to TARGET

mkdir -p ${TARGET}
rsync -av --progress ${SOURCE} ${TARGET} --exclude data --exclude output

cd ${TARGET}

ln -s ${OUTPUT} output
ln -s ${DATA} data

python run_job.py -c "$1"
