"""Example DAG: proves Airflow can launch a Spark job container. Build your own DAGs (Q17) the same way.

Airflow does not run Spark itself. Each task starts a short-lived container from the project's Spark image
(through the docker-proxy service), with this project folder mounted at /opt/project.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_DIR = os.environ["PROJECT_DIR"]          # absolute path of the project on the host

def spark_task(task_id, command, **kwargs):
    """A task that runs `command` in the Spark image, on the streambharat network."""
    return DockerOperator(
        task_id=task_id,
        image=os.environ.get("SPARK_IMAGE", "streambharat-etl-spark:1.0"),
        command=command,
        docker_url=os.environ.get("DOCKER_URL", "tcp://docker-proxy:2375"),
        network_mode=os.environ.get("DOCKER_NETWORK", "streambharat-net"),
        mounts=[Mount(source=PROJECT_DIR, target="/opt/project", type="bind")],
        mount_tmp_dir=False,
        auto_remove="success",
        environment={"KAFKA_BOOTSTRAP": "kafka:9092",
                     "PG_URL": "jdbc:postgresql://postgres:5432/ottdw", "PG_USER": "ott", "PG_PASSWORD": "ott"},
        **kwargs,
    )


with DAG(
    dag_id="sb_stack_check",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["streambharat", "example"],
) as dag:
    spark_task("stack_check", "python -m sbetl.smoke")
