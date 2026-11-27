# syntax=docker/dockerfile:1
# One image for the Spark master, the Spark worker, the workspace (Jupyter + spark-submit),
# the Kafka producers and the jobs Airflow launches.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Jars for Kafka, Delta Lake and Postgres are baked into the image, so no --packages download is needed.
RUN python3 - <<'EOF'
import urllib.request
maven = "https://repo1.maven.org/maven2"
jars = [
    "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
    "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar",
    "io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar",
    "io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar",
    "org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar",
]
for j in jars:
    name = j.rsplit("/", 1)[1]
    print("downloading", name)
    urllib.request.urlretrieve(f"{maven}/{j}", f"/opt/spark/jars/{name}")
EOF

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
 && pip3 install --no-cache-dir --no-deps delta-spark==3.2.1

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/project \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYTHONUNBUFFERED=1

WORKDIR /opt/project
