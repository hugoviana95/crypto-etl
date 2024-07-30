import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from pathlib import Path
from hook.binance_hook import BinanceHook
from hook.foxbit_hook import FoxbitHook
from hook.novadax_hook import NovadaxHook
import csv
import json



def to_timestamp(dt):
    dt = pendulum.parse(dt)
    return int(dt.timestamp())

def binance(end_time, start_time, file_path, symbol, interval):
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
            header = ["Open time", "Open price", "High", "Low", "Close price", "Volume", "Close time", "Quote asset volume", "Trades", "Taker buy base asset volume", "Taker buy quote asset volume", "Unused field, ignore."]
            csv_writer.writerow(header)
        # Registra o retorno da api
        csv_writer.writerows(r.json())

def foxbit(start_time, end_time, file_path, market_symbol, interval, limit=1):
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    file_exists = os.path.isfile(file_path) # Verifica existência do arquivo csv
    is_empty = os.path.getsize(file_path) == 0 if file_exists else True # Verifica se o arquivo csv está em branco
    with open(file_path, 'a', newline='') as file:
        csv_writer = csv.writer(file)
        r = FoxbitHook(
            start_time=to_timestamp(start_time)*1000,
            end_time=to_timestamp(end_time)*1000-1,
            market_symbol=market_symbol,
            interval=interval,
            limit=limit
        ).run()
        if not file_exists or is_empty:
            header = ["Open time", "Open price", "High", "Low", "Close price", "Close time", "Volume",  "Quote volume", "Number of trades", "Taker buy base volume", "Taker buy quote volume"]
            csv_writer.writerow(header)
        # Registra o retorno da api
        csv_writer.writerows(r.json())

def novadax(to_time, from_time, file_path, symbol, unit):
    (Path(file_path).parent).mkdir(parents=True, exist_ok=True)
    with open(file_path, 'a') as file:
        r = NovadaxHook(to_timestamp(from_time), to_timestamp(to_time), symbol, unit).run()
        json.dump(r.json(), file)
        file.write('\n')



# symbols que serão procurados
binance_symbols = ['BTCBRL', 'ETHBRL', 'SOLBRL']
foxbit_symbols = ['btcbrl', 'ethbrl', 'solbrl']
novadax_symbols = ['BTC_BRL', 'ETH_BRL', 'SOL_BRL']

with DAG(
    '15m_kandles',
    schedule_interval='*/15 * * * *',
    start_date=pendulum.now().subtract(minutes=30),
    catchup=True
):
    
    BASE_FOLDER = "../Datalake/bronze/{exchange}/15m_kandles/{symbol}"
    

    ## BINANCE
    binance_tasks = []
    for symbol in binance_symbols:
        binance_task = PythonOperator(
            task_id=f'binance_{symbol.lower()}',
            python_callable=binance,
            op_args=[
                "{{ data_interval_end }}",
                "{{ data_interval_start }}",
                os.path.join(BASE_FOLDER.format(exchange='binance', symbol=symbol.lower()), 'extract_date={{ ds }}.csv'),
                symbol,
                '15m'
            ]
        )
        binance_tasks.append(binance_task)


    ## FOXBIT
    foxbit_tasks = []
    for symbol in foxbit_symbols:
        foxbit_task = PythonOperator(
            task_id=f'foxbit_{symbol}',
            python_callable=foxbit,
            op_args=[
                "{{ data_interval_end }}",
                "{{ data_interval_start }}",
                os.path.join(BASE_FOLDER.format(exchange='foxbit', symbol=symbol), 'extract_date={{ ds }}.csv'),
                symbol,
                '15m'
            ]
        )
        foxbit_tasks.append(foxbit_task)


    ## NOVADAX
    novadax_tasks = []
    for symbol in novadax_symbols:
        novadax_task = PythonOperator(
            task_id=f'novadax_{symbol.replace('_', '').lower()}',
            python_callable=novadax,
            op_args=[
                "{{ data_interval_end }}",
                "{{ data_interval_start }}",
                os.path.join(BASE_FOLDER.format(exchange='novadax', symbol=symbol.replace('_', '').lower()), 'extract_date={{ ds }}.json'),
                symbol,
                'FIFTEEN_MIN'
            ]
        )
        novadax_tasks.append(novadax_task)



    # for task in binance_tasks:
    #     task >> 

    # for task in foxbit_tasks:
    #     task >> 

    # for task in novadax_tasks:
    #     task >> 