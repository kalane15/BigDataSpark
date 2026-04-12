from pyspark.sql.functions import col, sum, corr, desc, lit
from pyspark.sql.types import DoubleType, StructType, StructField, StringType
from log_execution.log_execution import log_execution
from pyspark.sql.functions import col, sum, corr, coalesce, lit


@log_execution
def highest_rated_products(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    dim_product = spark_app.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)

    max_rating = dim_product.agg({"product_rating": "max"}).collect()[0][0]
    result = dim_product.filter(col("product_rating") == max_rating) \
        .select("product_name", "product_price", "product_rating")

    result.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm6_highest_rated_products", properties=CH_PROPS)


@log_execution
def lowest_rated_products(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    dim_product = spark_app.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)

    min_rating = dim_product.agg({"product_rating": "min"}).collect()[0][0]
    result = dim_product.filter(col("product_rating") == min_rating) \
        .select("product_name", "product_price", "product_rating")

    result.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm6_lowest_rated_products", properties=CH_PROPS)


@log_execution
def rating_sales_correlation(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    dim_product = spark_app.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
    fact_sales = spark_app.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)

    sales_agg = fact_sales.groupBy("sale_product_id") \
        .agg(sum("sale_quantity").alias("total_sales_volume"))

    joined = dim_product.join(sales_agg, dim_product.sale_product_id == sales_agg.sale_product_id, how="left") \
        .select(col("product_rating"), col("total_sales_volume"))

    joined = joined.withColumn("total_sales_volume", coalesce(col("total_sales_volume"), lit(0)))

    corr_value = joined.select(corr("product_rating", "total_sales_volume")).collect()[0][0]
    if corr_value is None:
        corr_value = float('nan')

    result_df = spark_app.range(1).select(
        lit(corr_value).alias("correlation_value"),
        lit("Pearson correlation between product_rating and total_sales_volume").alias("description")
    )

    result_df.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm6_rating_sales_correlation", properties=CH_PROPS)


@log_execution
def most_reviewed_products(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    dim_product = spark_app.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)

    result = dim_product.filter(col("product_reviews").isNotNull()) \
        .orderBy(desc("product_reviews")) \
        .select("product_name", "product_reviews", "product_rating") \
        .limit(5)

    result.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm6_most_reviewed_products", properties=CH_PROPS)
