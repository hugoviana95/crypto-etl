import sys
import os
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from airflow import DAG
from airflow.operators.python import PythonOperator
from hook.mercado_hook import MercadoHook
import json
from pathlib import Path
import pendulum
import logging

LOGGER = logging.getLogger("airflow.task")

def to_timestamp(dt):
    dt = pendulum.parse(dt)
    return int(dt.timestamp())

def execute(to_time, from_time, file_path):
    LOGGER.info(f"To: {to_time}")
    LOGGER.info(f"From: {from_time}")
    LOGGER.info(f"To timestamp: {to_timestamp(to_time)} ")
    LOGGER.info(f"From timestamp: {to_timestamp(from_time)} ")
    (Path(file_path).parent).mkdir(parents=True, exist_ok=True)
    with open(file_path, 'a', newline='') as file:
        r = MercadoHook(
            symbol='BTC-BRL',
            resolution='1m',
            to_time=to_timestamp(to_time),
            from_time=to_timestamp(from_time),
        ).run()
        json.dump(r.json(), file)
        file.write("\n")


with DAG(
    'Mercado_1m',
    start_date=pendulum.datetime(2024, 7, 25, 12, 00, tz="UTC"),
    schedule='* * * * *',
):

    task_1m = PythonOperator(  
                        task_id='get_5m_info',
                        python_callable=execute,
                        op_args=[
                            "{{ data_interval_end }}",
                            "{{ data_interval_start }}",
                            "../Datalake/bronze/mercado/mercado_1m_{{ ds }}.json"
                        ]
                    )