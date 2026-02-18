FROM databricksruntime/python:16.4-LTS

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk procps curl wget tar \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN ln -sf /databricks/python3/bin/python3 /usr/bin/python3 && \
    ln -sf /databricks/python3/bin/pip3 /usr/bin/pip3

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Apache Spark 3.5.3 + Delta 3.3.2 (matches Databricks 16.4 LTS)
ENV SPARK_VERSION=3.5.3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=/databricks/python3/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/databricks/python3/bin/python3

RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN cd $SPARK_HOME/jars && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app
COPY . .
RUN uv pip install --python /databricks/python3/bin/python3 .

CMD ["/bin/bash"]
