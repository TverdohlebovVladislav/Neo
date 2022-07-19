FROM apache/airflow:2.2.3

# set root user
USER root

RUN apt-get update -qq && apt-get install vim -qqq
ENV AIRFLOW_HOME=/opt/airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG SPARK_VERSION="3.1.2"
ARG HADOOP_VERSION="3.2"

# Java is required in order to spark-submit work
# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    pip freeze && \
    java -version $$ \
    javac -version

# Setup JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME

ENV SPARK_HOME /usr/local/spark
RUN apt-get install wget -y
# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
        wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        mkdir -p "${SPARK_HOME}/bin" && \
        mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
        
# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin

EXPOSE 8080 5555 8793

# set airflow user 
USER airflow
