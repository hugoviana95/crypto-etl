import os
import sys
file_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(file_dir)

from airflow.models import BaseOperator, TaskInstance, DAG
from hook.binance_hook import BinanceHook
from pathlib import Path
import csv
from datetime import datetime



class BinanceOperator(BaseOperator):

    template_fields = ['file_path', 'start_time', 'end_time']

    def __init__(self, file_path, start_time, end_time, symbol, interval, **kwargs):
        self.file_path = file_path
        self.start_time = self.to_timestamp(start_time)
        self.end_time = self.to_timestamp(end_time)
        self.interval = interval
        self.symbol = symbol    
        super().__init__(**kwargs)

    
    def to_timestamp(self, dt):
        dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.00Z')
        timestamp = int(datetime.timestamp(dt))*1000
        return timestamp

    def execute(self, context):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True) # Cria pasta Datalake caso não exista
        file_exists = os.path.isfile(self.file_path) # Verifica existência do arquivo csv
        is_empty = os.path.getsize(self.file_path) == 0 if file_exists else True # Verifica se o arquivo csv está em branco
        with open(self.file_path, 'a', newline='') as file:
            r = BinanceHook(self.start_time, self.end_time, self.symbol, self.interval).run()
            data = list(r.json())
            csv_writer = csv.writer(file)
            if not file_exists or is_empty:
                # Escrever cabeçalho se o arquivo estiver vazio ou não existir
                header = ["Open time", "Open price", "High", "Low", "Close price", "Volume", "Close time", "Quote asset volume", "Trades", "Taker buy base asset volume", "Taker buy quote asset volume", "Unused field, ignore."]
                csv_writer.writerow(header)
            csv_writer.writerows(data)
