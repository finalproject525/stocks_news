
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

PRODUCER_CONFIGS = [
    {
        'api_name': 'yfinance',
        'dag_id': 'producer_partitioned_yfinance_dag',
        'topic': 'yfinance-data',
        'extra_env': {
            'PERIOD': '300d',
            'INTERVAL': '60m'
        }
    },
    {
        'api_name': 'finnhub',
        'dag_id': 'producer_partitioned_finnhub_dag',
        'topic': 'finnhub-data',
        'extra_env': {
            'FROM_DATE': '2025-01-01',
            'TO_DATE': '{{ ds }}'
        }
    }
]


def create_producer_dag(config):
    com_env = {
    'USE_SYMBOLS_TEST': 'True',
    'BROKER': 'kafka_spg:9092',
    'BATCH_SIZE': '50',
    'MODE': 'prod',
    'API_NAME': config['api_name'],
    'TOPIC': config['topic'],
    **config.get('extra_env', {})
    }


    dag = DAG(
        dag_id=config['dag_id'],
        default_args=default_args,
        description=f"Run {config['api_name']} producer container",
        schedule_interval='@hourly',
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=[config['api_name'], 'kafka', 'producer']
    )

    with dag:
        DockerOperator(
            task_id='run_producer',
            image='producer_spg',
            container_name=f"{config['api_name']}_producer_{{{{ ts_nodash }}}}",
            auto_remove=True,
            docker_url='unix://var/run/docker.sock',
            network_mode='net_stock_project',
            command='python -m producer.main_producer',
            environment=com_env,
            mount_tmp_dir=False
        )
    return dag

for config in PRODUCER_CONFIGS:
    globals()[config['dag_id']] = create_producer_dag(config)
