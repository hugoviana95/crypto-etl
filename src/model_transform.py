from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os

spark = SparkSession.builder.appName('model_transform').getOrCreate()

# Expande o caminho para o diretório home
input_path = os.path.expanduser('~/crypto-etl/Datalake/bronze/binance/5m_kandles/btcbrl')
output_path = os.path.expanduser('~/crypto-etl/Datalake/gold/model_csv')

df = spark.read.csv(input_path, header=True, inferSchema=True)
df = df.orderBy(f.col('Open time').desc()).limit(12)
df.write.csv(output_path, mode='overwrite', header=True)
