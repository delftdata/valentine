#!/bin/bash

# THIS SCRIPT INSTALLS ALL THE PROGRAMS REQUIRED BY THE VALENTINE BENCHMARK

sudo apt update && \

sudo apt install openjdk-8-jdk gcc g++ make parallel maven subversion patch docker.io dos2unix -y && \

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/ && \
export PATH=$PATH:${JAVA_HOME} && \

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

cd algorithms/coma && \
./build_coma.sh
