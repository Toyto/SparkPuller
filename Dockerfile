FROM ubuntu:latest
LABEL maintainer="masroorh"

# Install OpenJDK 8
RUN \
  apt-get update && \
  apt-get install -y openjdk-8-jdk && \
  rm -rf /var/lib/apt/lists/*

# Install Python
RUN \
    apt-get update && \
    apt-get install -y python python-dev python-pip python-virtualenv && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark and Numpy
RUN \
    pip install numpy && \
    pip install pyspark

COPY postgresql-42.2.8.jar .
ENV SPARK_CLASSPATH=/postgresql-42.2.8.jar
# Define working directory
WORKDIR /data

# Define default command
CMD ["bash"]