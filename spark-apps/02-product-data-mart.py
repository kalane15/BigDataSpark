from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.functions import sum


def top_10_products():
    fact_sales = spark.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)
    dim_product = spark.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)

    top_products = fact_sales.join(dim_product, "sale_product_id") \
        .groupBy("product_name", "product_price") \
        .agg(
        sum("sale_quantity").alias("total_quantity_sold"),
        sum("sale_total_price").alias("total_revenue")
    ) \
        .orderBy(col("total_revenue").desc()) \
        .select(["product_name", "product_price", "total_quantity_sold", "total_revenue"]) \
        .limit(10)

    top_products.write \
        .jdbc(url=CH_URL, mode="overwrite", table="top_10_products", properties=CH_PROPS)


def revenue_by_category():
    fact_sales = spark.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)
    dim_product = spark.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)

    revenue_by_cat = fact_sales.join(dim_product, "sale_product_id") \
        .groupBy("product_category") \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .select(col("product_category").alias("category_name"), col("total_revenue"))

    revenue_by_cat.write \
        .jdbc(url=CH_URL, mode="overwrite", table="revenue_by_category", properties=CH_PROPS)


def product_reviews():
    dim_product = spark.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)

    product_rating_reviews = dim_product.groupBy("product_name", "product_price") \
        .agg(
        avg("product_rating").alias("avg_rating"),
        sum("product_reviews").alias("total_reviews")
    ).select("product_name", "product_price", "avg_rating", "total_reviews")

    product_rating_reviews.write \
        .jdbc(url=CH_URL, mode="overwrite", table="product_rating_reviews", properties=CH_PROPS)


if __name__ == "__main__":
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

    print("Загрузка top_10_products")
    top_10_products()
    print("Загрузка revenue_by_category")
    revenue_by_category()
    print("Загрузка product_reviews")
    product_reviews()
    spark.stop()
