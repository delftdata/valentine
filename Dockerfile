FROM ubuntu:18.04
LABEL maintainer="virtualda@gmail.com"

RUN apt update && apt install -y \
    apt-transport-https \
    curl \
    software-properties-common \
    wget \
    openjdk-8-jre \ 
    build-essential

################################################################################
# Install Python

RUN add-apt-repository -y ppa:deadsnakes/ppa

RUN apt update && apt install -y \
    python3.8-dev \
    python3.8-distutils

# Install pip via PyPA's recommended way rather than the outdated apt repos
# See: https://pip.pypa.io/en/stable/installing/
RUN curl https://bootstrap.pypa.io/get-pip.py -o ./get-pip.py && \
    python3.8 get-pip.py

# Upgrade pip and install virtualenv
# Default pip is python3.6 version
RUN python3.8 -m pip install -U pip 

COPY requirements.txt /tmp/

RUN python3.8 -m pip install -r /tmp/requirements.txt --no-cache-dir --ignore-installed

RUN python3.8 -m nltk.downloader stopwords
RUN python3.8 -m nltk.downloader punkt
RUN python3.8 -m nltk.downloader wordnet

RUN apt-get install -y unzip curl 

RUN apt-get install -y openjdk-8-jdk

RUN apt-get install -y apt-transport-https

RUN wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch |  apt-key add -

RUN echo "deb https://artifacts.elastic.co/packages/6.x/apt stable main" |  tee /etc/apt/sources.list.d/elastic-6.x.list

RUN apt-get update && apt-get install -y elasticsearch systemd

RUN cd /usr/local/bin  && ln -s /usr/bin/python3.8 python

RUN apt-get install -y rsync

RUN echo vm.max_map_count=262144 >> /etc/sysctl.conf

CMD ["python3.8"]

# docker login etc in order to login on the docker hub and to be able to upload images
# docker build .
# docker images
# docker tag ?????? asteriosk/sempropenv:3.8.5
# docker push asteriosk/sempropenv
#

