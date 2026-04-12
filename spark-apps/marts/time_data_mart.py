from pyspark.sql.functions import col, sum, count, avg, year, month, lag
from pyspark.sql.window import Window
from log_execution.log_execution import log_execution

@log_execution
def revenue_comparsion(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)

    df = (fact_sales.filter(col("sale_date").isNotNull())
        .withColumn("year", year("sale_date"))
        .withColumn("month", month("sale_date"))
        .groupBy("year", "month")
        .agg(
        sum("sale_total_price").alias("total_revenue"),
        count("*").alias("total_orders"),
        avg("sale_total_price").alias("avg_order_value")
        )
        .orderBy("year", "month"))

    df.write.jdbc(url=CH_URL, mode="overwrite", table="dm3_revenue_comparsion", properties=CH_PROPS)


@log_execution
def trends(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)

    df_year = fact_sales.filter(col("sale_date").isNotNull()) \
        .withColumn("year", year("sale_date")) \
        .withColumn("month", month("sale_date")) \
        .groupBy("year", "month") \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .orderBy("year", "month")

    window_spec = Window.orderBy("year")
    df_with_change = df_year.withColumn("prev_revenue", lag("total_revenue").over(window_spec)) \
        .withColumn("revenue_change_percent",
                    (col("total_revenue") - col("prev_revenue")) / col("prev_revenue") * 100) \
        .select("year", "month", "total_revenue", "revenue_change_percent")

    df_with_change.write.jdbc(url=CH_URL, mode="overwrite", table="dm3_trends", properties=CH_PROPS)


@log_execution
def monthly_avg_order_value(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)

    df = fact_sales.filter(col("sale_date").isNotNull()) \
        .withColumn("year", year("sale_date")) \
        .withColumn("month", month("sale_date")) \
        .groupBy("year", "month") \
        .agg(avg("sale_total_price").alias("avg_order_value")) \
        .orderBy("year", "month")

    df.write.jdbc(url=CH_URL, mode="overwrite", table="dm3_monthly_avg_order_value", properties=CH_PROPS)
