from pyspark.sql.functions import col, sum, count, avg
from pyspark.sql.functions import concat, lit
from log_execution import log_execution


@log_execution
def customers_by_country(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    dim_customer = spark_app.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPS)

    df = dim_customer.groupBy("customer_country") \
        .agg(count("*").alias("customer_count")) \
        .select(col("customer_country").alias("country_name"), "customer_count") \
        .orderBy("country_name")

    df.write.jdbc(url=CH_URL, mode="overwrite", table="customers_by_country", properties=CH_PROPS)


@log_execution
def top_10_customers(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)
    dim_customer = spark_app.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPS)

    df = fact_sales.join(dim_customer, "sale_customer_id") \
        .groupBy(dim_customer.customer_email, dim_customer.customer_first_name, dim_customer.customer_last_name) \
        .agg(sum("sale_total_price").alias("total_spent")) \
        .withColumn("customer_name", concat(col("customer_first_name"), lit(" "), col("customer_last_name"))) \
        .select("customer_email", "customer_name", "total_spent") \
        .orderBy(col("total_spent").desc()) \
        .limit(10)

    df.write.jdbc(url=CH_URL, mode="overwrite", table="top_10_customers_by_total", properties=CH_PROPS)


@log_execution
def avg_check_per_customer(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)
    dim_customer = spark_app.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPS)

    df = fact_sales.join(dim_customer, "sale_customer_id") \
        .groupBy(dim_customer.customer_email, dim_customer.customer_first_name, dim_customer.customer_last_name) \
        .agg(avg("sale_total_price").alias("avg_check")) \
        .withColumn("customer_name", concat(col("customer_first_name"), lit(" "), col("customer_last_name"))) \
        .select("customer_email", "customer_name", "avg_check")

    df.write.jdbc(url=CH_URL, mode="overwrite", table="avg_check_per_customer", properties=CH_PROPS)
