from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--symbols', required=True)
parser.add_argument('--base_folder', required=True)
args = parser.parse_args()

SYMBOLS = args.symbols.split(',')

file_exist = False
# Confere se os arquivos necessários já estão no datalake
have_predictions_files = os.path.isdir(os.path.join(args.base_folder, f'datalake/silver/predictions'))
have_history_files = os.path.isdir(os.path.join(args.base_folder, f'datalake/bronze/binance/5m_kandles'))
if have_history_files and have_predictions_files:
    file_exist = True

# Se existir, segue com a execução
if file_exist:
    spark = SparkSession.builder.appName('dash_transform').getOrCreate()

    dfs = []
    # Loop que faz a leitura das bases de cada moeda
    for symbol in SYMBOLS:
        # Leitura das predições
        predictions = spark.read.csv(os.path.join(args.base_folder, f'datalake/silver/predictions/5m_predictions_{symbol.lower()}.csv'), header=True, inferSchema=True)
        # Leitura dos dados históricos
        history = spark.read.csv(os.path.join(args.base_folder, f'datalake/bronze/binance/5m_kandles/{symbol.lower()}'), header=True, inferSchema=True)
        # Junção dos dados históricos com as predições
        df = history.join(predictions, on='Close_time', how='outer')
        # Cria coluna com datetime da hora do fechamento
        df = df.withColumn('Close_datetime', f.from_unixtime((df['Close_time'] + 1)/1000))
        # Filtra os valores da últimas 12 horas
        df = df.orderBy(f.col('Close_time').desc()).limit(144)
        # Cria coluna com o symbol
        df = df.withColumn('symbol', f.lit(symbol))
        dfs.append(df)

    # Loop para fazer a junção de todas as bases em uma tabela só
    final_df = dfs[0]
    for df in dfs[1:]:
        final_df = final_df.union(df)

    final_df.write.csv(os.path.join(args.base_folder, f'datalake/gold/dashboard'), mode='overwrite', header=True)