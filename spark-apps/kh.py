from pyspark.sql import SparkSession

# Параметры подключения к ClickHouse
ch_url = "jdbc:clickhouse://clickhouse:8123/default"
ch_properties = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

spark = SparkSession.builder.appName("ReadClickHouse").getOrCreate()

# Читаем таблицу в DataFrame
df = spark.read.jdbc(url=ch_url, table="page_views", properties=ch_properties)

df.show()

# Преобразуем Spark DataFrame в pandas (если данных немного)
pandas_df = df.toPandas()
print(pandas_df.head())

spark.stop()