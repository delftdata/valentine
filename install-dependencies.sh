#!/bin/bash

# THIS SCRIPT INSTALLS ALL THE PROGRAMS REQUIRED BY THE VALENTINE BENCHMARK

sudo apt update && \

sudo apt install build-essential parallel maven subversion docker.io dos2unix -y && \

sudo systemctl start docker && \
sudo systemctl enable docker && \

wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
sudo bash ~/miniconda.sh -u -b -p $HOME/miniconda && \
eval "$(""$HOME""/miniconda/bin/conda shell.bash hook)" && \
conda init && \
sudo "$HOME""/miniconda/bin/conda" create -n valentine-suite python=3.8 -y --force && \
conda activate valentine-suite && \

pip install -r requirements.txt && \

python -m nltk.downloader stopwords && \
python -m nltk.downloader punkt  && \
python -m nltk.downloader wordnet && \

sudo apt install openjdk-8-jdk -y && \

cd algorithms/coma && \
./build_coma.sh
