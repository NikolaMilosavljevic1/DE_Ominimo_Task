from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

CONFIG_PATH = "/config/pipeline.json"
IMAGE = "ominimo-pipeline:latest"

# adjust these to match where data and config live on the host
DATA_HOST_PATH = "/opt/ominimo/data"
CONFIG_HOST_PATH = "/opt/ominimo/config"

with DAG(
    dag_id="ominimo_motor_ingestion",
    description="Daily motor insurance policy ingestion pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ominimo", "motor", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_pipeline = DockerOperator(
        task_id="run_motor_ingestion_pipeline",
        image=IMAGE,
        command=f"python3 /app/src/main.py --config {CONFIG_PATH} --dataflow motor-ingestion",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(source=DATA_HOST_PATH,   target="/data",   type="bind"),
            Mount(source=CONFIG_HOST_PATH, target="/config", type="bind", read_only=True),
        ],
        environment={
            "PYTHONPATH": "/app/src",
        },
        retrieve_output=True,
    )

    start >> run_pipeline >> end
