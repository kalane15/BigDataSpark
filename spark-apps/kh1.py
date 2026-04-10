from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum

PG_URL = "jdbc:postgresql://postgres:5432/postgres"
PG_PROPS = {
    "user": "postgres",
    "password": "mysecretpassword",
    "driver": "org.postgresql.Driver"
}
CH_URL = "jdbc:clickhouse://clickhouse:8123/default"
CH_PROPS = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

spark = SparkSession.builder \
    .appName("LoadDataWarehouse") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

mock_df = spark.read.jdbc(url=PG_URL, table="mock_data", properties=PG_PROPS)

fact_sales = spark.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)
dim_product = spark.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)

top_products = fact_sales.join(dim_product, "sale_product_id") \
    .groupBy("product_name") \
    .agg(
    sum("sale_quantity").alias("total_quantity_sold"),
    sum("sale_total_price").alias("total_revenue")
) \
    .orderBy(col("total_revenue").desc()) \
    .limit(10)

top_products.write \
    .jdbc(url=CH_URL, mode="overwrite", table="top_10_products", properties=CH_PROPS)
