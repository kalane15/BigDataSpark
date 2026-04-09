# spark-apps/list_tables.py
from pyspark.sql import SparkSession

pg_url = "jdbc:postgresql://postgres:5432/postgres"
pg_properties = {
    "user": "postgres",
    "password": "mysecretpassword",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("PostgresListTables") \
    .getOrCreate()

# Правильный подзапрос с алиасом
query = """
    (SELECT table_name 
     FROM information_schema.tables 
     WHERE table_schema = 'public') AS t
"""

df = spark.read.jdbc(url=pg_url, table=query, properties=pg_properties)

print("Список таблиц в базе postgres (схема public):")
df.show(truncate=False)

spark.stop()