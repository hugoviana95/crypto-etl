import os
import sys
file_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(file_dir)

from datetime import datetime, timedelta
import pytz
from airflow import DAG
from operators.binance_operator import BinanceOperator  # Certifique-se de importar corretamente o operador personalizado

# Configuração da timezone
TIME_ZONE = pytz.timezone('America/Sao_Paulo')

# Função para obter o horário de início e fim
def get_time_range():
    now = datetime.now(tz=TIME_ZONE)
    start_time = int(datetime.timestamp((now - timedelta(seconds=60))) * 1000)
    end_time = int(datetime.timestamp(now) * 1000)
    return start_time, end_time

with DAG(
    'BinanceDAG',
    start_date=datetime.now(),
    schedule_interval='* * * * *',  # A cada minuto
    catchup=False # O atributo catchup no Airflow determina se o DAG deve tentar "recuperar" execuções passadas que foram perdidas (por exemplo, se o Airflow estava desligado ou se houve um problema).
) as dag:

    start_time, end_time = get_time_range()

    op = BinanceOperator(
        file_path="../Datalake/binance_1m.csv",
        start_time=start_time,
        end_time=end_time,
        symbol='BTCBRL',
        interval='1m',
        task_id='test',
    )