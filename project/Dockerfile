# Базовый образ Airflow
FROM apache/airflow:2.8.1-python3.11

# Установка зависимостей из requirements.txt
COPY requirements.txt /tmp
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Используем root для установки дополнительных пакетов
USER root

# Установка Java (OpenJDK 11)
RUN apt-get update && apt-get install -y default-jdk wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Установка Apache Spark
RUN mkdir -p /opt/spark && \
    cd /opt/spark && \
    wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar xzfv spark-3.5.1-bin-hadoop3.tgz && \
    rm spark-3.5.1-bin-hadoop3.tgz

# Установка переменных среды для Java и Spark
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV SPARK_HOME="/opt/spark/spark-3.5.1-bin-hadoop3"
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
