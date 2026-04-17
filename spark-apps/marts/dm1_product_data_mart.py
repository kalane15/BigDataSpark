from pyspark.sql.functions import col, avg
from pyspark.sql.functions import sum
from log_execution.log_execution import log_execution


@log_execution
def top_10_products(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)
    dim_product = spark_app.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)

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
        .jdbc(url=CH_URL, mode="overwrite", table="dm1_top_10_products", properties=CH_PROPS)


@log_execution
def revenue_by_category(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)
    dim_product = spark_app.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)

    revenue_by_cat = fact_sales.join(dim_product, "sale_product_id") \
        .groupBy("product_category") \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .select(col("product_category").alias("category_name"), col("total_revenue"))

    revenue_by_cat.write \
        .jdbc(url=CH_URL, mode="overwrite", table="dm1_revenue_by_category", properties=CH_PROPS)


@log_execution
def product_reviews(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    dim_product = spark_app.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)

    product_rating_reviews = dim_product.groupBy("product_name", "product_price") \
        .agg(
        avg("product_rating").alias("avg_rating"),
        sum("product_reviews").alias("total_reviews")
    ).select("product_name", "product_price", "avg_rating", "total_reviews")

    product_rating_reviews.write \
        .jdbc(url=CH_URL, mode="overwrite", table="dm1_product_rating_reviews", properties=CH_PROPS)
