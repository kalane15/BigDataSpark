from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg
from pyspark.sql.functions import concat, lit


def log_execution(func):
    def wrapper(*args, **kwargs):
        print(f"Загрузка {func.__name__}...")
        return func(*args, **kwargs)

    return wrapper


@log_execution
def customers_by_country():
    dim_customer = spark.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPS)

    df = dim_customer.groupBy("customer_country") \
        .agg(count("*").alias("customer_count")) \
        .select(col("customer_country").alias("country_name"), "customer_count") \
        .orderBy("country_name")

    df.write.jdbc(url=CH_URL, mode="overwrite", table="customers_by_country", properties=CH_PROPS)


@log_execution
def top_10_customers():
    fact_sales = spark.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)
    dim_customer = spark.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPS)

    df = fact_sales.join(dim_customer, "sale_customer_id") \
        .groupBy(dim_customer.customer_email, dim_customer.customer_first_name, dim_customer.customer_last_name) \
        .agg(sum("sale_total_price").alias("total_spent")) \
        .withColumn("customer_name", concat(col("customer_first_name"), lit(" "), col("customer_last_name"))) \
        .select("customer_email", "customer_name", "total_spent") \
        .orderBy(col("total_spent").desc()) \
        .limit(10)

    df.write.jdbc(url=CH_URL, mode="overwrite", table="top_10_customers_by_total", properties=CH_PROPS)


@log_execution
def avg_check_per_customer():
    fact_sales = spark.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)
    dim_customer = spark.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPS)

    df = fact_sales.join(dim_customer, "sale_customer_id") \
        .groupBy(dim_customer.customer_email, dim_customer.customer_first_name, dim_customer.customer_last_name) \
        .agg(avg("sale_total_price").alias("avg_check")) \
        .withColumn("customer_name", concat(col("customer_first_name"), lit(" "), col("customer_last_name"))) \
        .select("customer_email", "customer_name", "avg_check")

    df.write.jdbc(url=CH_URL, mode="overwrite", table="avg_check_per_customer", properties=CH_PROPS)


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
        .appName("ClientMart") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    top_10_customers()
    customers_by_country()
    avg_check_per_customer()
    spark.stop()
