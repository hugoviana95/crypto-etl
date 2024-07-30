from pyspark.sql import SparkSession
from pyspark.sql import functions as f
spark = SparkSession.builder.appName('model_transform').getOrCreate()

df = spark.read.csv('../Datalake/bronze/binance/5m_kandles/btcbrl', header=True, inferSchema=True)
df = df.orderBy(f.col('Open time').desc()).limit(12)
df.write.csv('../Datalake/gold/model_csv', mode='overwrite', header=True)