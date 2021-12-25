FROM ubuntu:latest

RUN apt-get update &&\ 
    apt-get install -y wget &&\
    apt-get install -y openjdk-8-jdk-headless && \
    apt-get install -y gnupg curl nano && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y sbt &&\
    apt-get install -y netcat &&\
    apt-get install -y screen && \
    apt-get install -y python3  &&\
    apt-get install -y python3-pip  &&\
    pip3 install jupyterlab==3.2.5 pandas==1.3.5 matplotlib bokeh==2.4.2


RUN useradd -m -d /home/developer/ -s /bin/bash -G sudo developer

USER developer
WORKDIR /home/developer/

RUN wget https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar zxvf spark-3.1.2-bin-hadoop3.2.tgz  && \
    mkdir shared_with_host
