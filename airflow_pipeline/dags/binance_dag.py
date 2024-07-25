import os
import sys
file_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(file_dir)

from airflow import DAG
from airflow.operators.python import PythonOperator
import csv
from hook.binance_hook import BinanceHook
from pathlib import Path
import pendulum
import logging

# LOGGER = logging.getLogger("airflow.task")

def to_timestamp(dt):
    dt = pendulum.parse(dt)
    return int(dt.timestamp()*1000)

def execute(end_time, start_time, file_path):
    (Path(file_path).parent).mkdir(parents=True, exist_ok=True)
    file_exists = os.path.isfile(file_path) # Verifica existência do arquivo csv
    is_empty = os.path.getsize(file_path) == 0 if file_exists else True # Verifica se o arquivo csv está em branco
    with open(file_path, 'a', newline='') as file:
        csv_writer = csv.writer(file)
        # Chama a api pelo hook
        r = BinanceHook(
            start_time=to_timestamp(start_time),
            end_time=to_timestamp(end_time)-1,
            symbol='BTCBRL',
            interval='5m',
            time_zone='-3'
        ).run()
        # Escrever cabeçalho se o arquivo estiver vazio ou não existir
        if not file_exists or is_empty:
            header = ["Open time", "Open price", "High", "Low", "Close price", "Volume", "Close time", "Quote asset volume", "Trades", "Taker buy base asset volume", "Taker buy quote asset volume", "Unused field, ignore."]
            csv_writer.writerow(header)
        # Registra o retorno da api
        csv_writer.writerows(r.json())



with DAG(
    'Binance_5m',
    start_date=pendulum.datetime(2024, 7, 24, 23, 40, tz="UTC"),
    schedule='*/5 * * * *',
    # catchup=False
):

    task_1m = PythonOperator(
                            task_id='get_5m_info',
                            python_callable=execute,
                            op_args=[
                                "{{ data_interval_end }}",
                                "{{ data_interval_start }}",
                                "../Datalake/bronze/binance/binance_5m_{{ ds }}.csv",
                            ]
                        )