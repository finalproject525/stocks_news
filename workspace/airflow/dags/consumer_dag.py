from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import os


import logging

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

CONSUMER_CONFIGS = [
    {
        'api_name': 'yfinance',
        'dag_id': 'consumer_partitioned_yfinance_dag',
        'topic': 'yfinance-data'
    },
    {
        'api_name': 'finnhub',
        'dag_id': 'consumer_partitioned_finnhub_dag',
        'topic': 'finnhub-data'
    }
]


def create_consumer_dag(config):
    dag = DAG(
        dag_id=config['dag_id'],
        default_args=default_args,
        description=f"Run {config['api_name']} consumer containers (partitioned)",
        schedule_interval='@hourly',
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=[config['api_name'], 'kafka', 'consumer']
    )

    with dag:
        # json,parquet
        common_env = {
            'TOPIC': config['topic'],
            'MODE': 'prod',
            'USE_MINIO': os.getenv("USE_MINIO", "True"),
            'BROKER': os.getenv("BROKER", "kafka_spg:9092"),
            'BUCKET': os.getenv("BUCKET", "final-de-project-sp500"),
            'OUTPUT_FORMATS': os.getenv("OUTPUT_FORMATS", "json"),
            'AWS_ACCESS_KEY_ID': os.getenv("AWS_ACCESS_KEY_ID", ""),
            'AWS_SECRET_ACCESS_KEY': os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            'AWS_DEFAULT_REGION': os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            'MINIO_ROOT_USER': os.getenv("MINIO_ROOT_USER", "minioadmin"),
            'MINIO_ROOT_PASSWORD': os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            'MINIO_ENDPOINT': os.getenv("MINIO_ENDPOINT", "http://minio_spg:9000"),
        }

       
        for i in range(2):  
            DockerOperator(
                task_id=f"consumer_partition_{i}",
                image="consumer_spg",
                container_name=f"{config['api_name']}_consumer_p{i}_{{{{ ts_nodash }}}}",
                auto_remove=True,
                docker_url="unix://var/run/docker.sock",
                network_mode="net_stock_project",
                command="python -m consumer.main_consumer",
                environment={**common_env, 'PARTITION_ID': str(i)},
                mount_tmp_dir=False
            )

    return dag



for config in CONSUMER_CONFIGS:
    globals()[config['dag_id']] = create_consumer_dag(config)
