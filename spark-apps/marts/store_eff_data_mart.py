from pyspark.sql.functions import col, sum, avg, count
from log_execution.log_execution import log_execution


@log_execution
def top_stores_by_revenue(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
    dim_store = spark_app.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)

    result = fact_sales.join(dim_store, "sale_store_id") \
        .groupBy("store_name", "store_email") \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .orderBy(col("total_revenue").desc()) \
        .limit(5)

    result.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm4_top_stores_by_revenue", properties=CH_PROPS)


@log_execution
def sales_by_city(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
    dim_store = spark_app.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)

    result = fact_sales.join(dim_store, "sale_store_id") \
        .groupBy("store_city") \
        .agg(
        sum("sale_total_price").alias("total_sales"),
        sum("sale_quantity").alias("total_quantity")
    ) \
        .withColumnRenamed("store_city", "city")

    result.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm4_sales_by_city", properties=CH_PROPS)


@log_execution
def sales_by_country(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
    dim_store = spark_app.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)

    result = fact_sales.join(dim_store, "sale_store_id") \
        .groupBy("store_country") \
        .agg(
        sum("sale_total_price").alias("total_sales"),
        sum("sale_quantity").alias("total_quantity")
    ) \
        .withColumnRenamed("store_country", "country")

    result.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm4_sales_by_country", properties=CH_PROPS)


@log_execution
def avg_check_per_store(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
    dim_store = spark_app.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)

    result = fact_sales.join(dim_store, "sale_store_id") \
        .groupBy("store_name", "store_email") \
        .agg(
        avg("sale_total_price").alias("avg_check_amount"),
        count("*").alias("total_orders")
    )

    result.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm4_avg_check_by_store", properties=CH_PROPS)
