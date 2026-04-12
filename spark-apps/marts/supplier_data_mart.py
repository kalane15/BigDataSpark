from pyspark.sql.functions import col, sum, avg
from log_execution.log_execution import log_execution


@log_execution
def top_suppliers_by_revenue(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
    dim_product = spark_app.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
    dim_supplier = spark_app.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)

    result = fact_sales.join(dim_product, "sale_product_id") \
        .join(dim_supplier, dim_product.product_supplier_id == dim_supplier.product_supplier_id) \
        .groupBy(dim_supplier.supplier_name) \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .orderBy(col("total_revenue").desc()) \
        .limit(5)

    result.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm5_top_suppliers_by_revenue", properties=CH_PROPS)


@log_execution
def avg_price_by_supplier(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    dim_product = spark_app.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
    dim_supplier = spark_app.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)

    result = dim_product.join(dim_supplier, "product_supplier_id") \
        .groupBy(dim_supplier.supplier_name, "supplier_email") \
        .agg(avg("product_price").alias("avg_product_price"))

    result.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm5_avg_price_by_supplier", properties=CH_PROPS)


@log_execution
def sales_by_supplier_country(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS):
    fact_sales = spark_app.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
    dim_product = spark_app.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
    dim_supplier = spark_app.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)

    result = fact_sales.join(dim_product, "sale_product_id") \
        .join(dim_supplier, "product_supplier_id") \
        .groupBy(dim_supplier.supplier_country.alias("country")) \
        .agg(
        sum("sale_total_price").alias("total_sales"),
        sum("sale_quantity").alias("total_quantity")
    )

    result.write \
        .mode("overwrite") \
        .jdbc(CH_URL, "dm5_sales_by_supplier_country", properties=CH_PROPS)
