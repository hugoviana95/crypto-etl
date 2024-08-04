from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os
import argparse
from pyspark.sql.window import Window

parser = argparse.ArgumentParser()
parser.add_argument('--symbols', required=True)
parser.add_argument('--base_folder', required=True)
args = parser.parse_args()

SYMBOLS = args.symbols.split(',')

spark = SparkSession.builder.appName('model_transform').getOrCreate()

for symbol in SYMBOLS:
    input_path = os.path.join(args.base_folder, f'datalake/bronze/binance/5m_kandles/{symbol.lower()}')
    output_path = os.path.join(args.base_folder, f'datalake/silver/5m_models_data/{symbol.lower()}')

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    if df.count() >= 2: # Ter pelo menos 2 registros para gerar os dados de predição
        df = df.withColumn('kandle_size', f.col('Close_price') - f.col('Open_price'))

        features = [
            'Close_price',
            'High',
            'Low',
            'Volume',
            'Trades', 
            'kandle_size',
            'Taker_buy_base_asset_volume',
            'Taker_buy_quote_asset_volume',
            'Open_time',
        ]

        # Cria as lag features
        for feature in features:
            df = df.withColumn(f"{feature}_lag_1", f.lag(f.col(feature), 1).over(Window.orderBy("Close_time")))

        # Seleciona somente as lag features
        lag_columns = [col for col in df.columns if 'lag' in col]
        df = df.select(*lag_columns)

        df = df.orderBy(f.col('Open_time').desc()).limit(1) # Busca o valor mais recente
        df.write.csv(output_path, mode='overwrite', header=True)