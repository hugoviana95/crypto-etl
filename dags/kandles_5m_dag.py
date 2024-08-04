import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import csv
import pandas as pd
from pathlib import Path
import pendulum
import glob
from hook.binance_hook import BinanceHook
import joblib

    
LOG = LoggingMixin().log

def to_timestamp(dt):
    dt = pendulum.parse(dt)
    return int(dt.timestamp())

def get_data(end_time, start_time, file_path, symbol, interval):
    LOG.info(file_path)
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    file_exists = os.path.isfile(file_path) # Verifica existência do arquivo csv
    is_empty = os.path.getsize(file_path) == 0 if file_exists else True # Verifica se o arquivo csv está em branco
    with open(file_path, 'a', newline='') as file:
        csv_writer = csv.writer(file)
        # Chama a api pelo hook
        r = BinanceHook(
            start_time=to_timestamp(start_time)*1000,
            end_time=to_timestamp(end_time)*1000-1,
            symbol=symbol,
            interval=interval,
        ).run()
        # Escrever cabeçalho se o arquivo estiver vazio ou não existir
        if not file_exists or is_empty:
            header = ["Open_time", "Open_price", "High", "Low", "Close_price", "Volume", "Close_time", "Quote_asset_volume", "Trades", "Taker_buy_base_asset_volume", "Taker_buy_quote_asset_volume", "Unused_field_ignore"]
            csv_writer.writerow(header)
        # Registra o retorno da api
        csv_writer.writerows(r.json())

def make_prediction(symbol):
    # Caminho para o arquivo gerado pelo ETL
    file_path = os.path.join(BASE_FOLDER, f'datalake/silver/5m_models_data/{symbol}', '*.csv')

    # Leitura do arquivo csv com os dados em tempo real do mercado
    csv_file = glob.glob(file_path)
    if len(csv_file) >= 1: # Confere se o arquivo com os dados para predição foi criado
        atual_data = pd.read_csv(csv_file[0])

        # Carrega modelo
        model = joblib.load(os.path.join(BASE_FOLDER, f'models/5m_model_{symbol}.pkl'))
        
        # Gera a predição pro próximo fechamento
        predict = model.predict(atual_data)
        
        # Regista a predição no arquivo csv
        file_path = os.path.join(BASE_FOLDER, f'datalake/silver/predictions/5m_predictions_{symbol}.csv')
        file_exists = os.path.isfile(file_path) # Verifica existência do arquivo csv
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        if file_exists:
            with open(file_path, 'a') as file:
                csv_writer = csv.writer(file)
                csv_writer.writerow([atual_data['Open_time_lag_1'].values[0]+899999, predict[0]])
        else:
            with open(file_path, 'w') as file:
                csv_writer = csv.writer(file)
                csv_writer.writerow(['Close_time', 'Predict'])
                csv_writer.writerow([atual_data['Open_time_lag_1'].values[0]+899999, predict[0]])
    



# symbols que serão procurados
SYMBOLS = ['BTCBRL', 'ETHBRL', 'BNBBRL']
BASE_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERVAL = '5m'

with DAG(
    '5m_kandles',
    schedule_interval='*/5 * * * *',
    start_date=pendulum.now().subtract(minutes=30),
    catchup=True,
    max_active_runs=1
):

    get_data_tasks = []
    for symbol in SYMBOLS:
        # Tasks de coleta na api da binance para cada symbol
        get_binance_data = PythonOperator(
            task_id=f'get_binance_data_{symbol.lower()}',
            python_callable=get_data,
            op_args=[
                "{{ data_interval_end }}",
                "{{ data_interval_start }}",
                os.path.join(BASE_FOLDER, 'datalake/bronze/binance/5m_kandles', symbol.lower(), 'extract_date={{ ds }}.csv' ),
                symbol,
                '5m'
            ]
        )
        get_data_tasks.append(get_binance_data)

    # Task de criação do csv para o modelo de predição
    model_transform = SparkSubmitOperator(task_id=f'model_transform_{symbol.lower()}',
                                            application=os.path.join(BASE_FOLDER, 'src/spark/model_transform.py'),
                                            name='model_transform',
                                            application_args=['--symbols', ','.join(SYMBOLS), '--base_folder', BASE_FOLDER])

    predict_tasks = []
    for symbol in SYMBOLS:
        # Task para atualizar a previsão do fechamento do próximo ciclo
        model_predict = PythonOperator(task_id=f'make_prediction_{symbol.lower()}',
                                    python_callable=make_prediction,
                                    op_args=[symbol.lower()])
        predict_tasks.append(model_predict)


    dash_transform = SparkSubmitOperator(task_id='dashboard_transform',
                                         application=os.path.join(BASE_FOLDER, 'src/spark/dash_transform.py'),
                                         name='dashboard_transform',
                                         application_args=['--symbols', ','.join(SYMBOLS), '--base_folder', BASE_FOLDER])
    
        
    # Define pré-requisitos de execução
    for task in get_data_tasks:
        task >> model_transform

    for task in predict_tasks:
        model_transform >> task >> dash_transform