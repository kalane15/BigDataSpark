from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, try_to_date, row_number
from pyspark.sql.window import Window
from marts.client_sell_data_mart import *
from marts.product_data_mart import *
from marts.time_data_mart import *
from marts.store_eff_data_mart import *
from marts.supplier_data_mart import *
from marts.product_quality_data_mart import *

from log_execution.log_execution import log_execution


def write_table(df, table_name):
    df.write.mode("append").jdbc(url=PG_URL, table=table_name, properties=PG_PROPS)


def read_table(table_name):
    return spark_app.read.jdbc(url=PG_URL, table=table_name, properties=PG_PROPS)


def deduplicate(df, partition_cols, order_cols):
    window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
    return df.withColumn("_rn", row_number().over(window_spec)).filter(col("_rn") == 1).drop("_rn")


@log_execution
def load_dim_customer_pet():
    dim_pet = mock_df.select(
        "customer_pet_type", "customer_pet_name", "customer_pet_breed"
    ).distinct().filter(col("customer_pet_type").isNotNull())
    write_table(dim_pet, "dim_customer_pet")
    dim_pet_with_id = read_table("dim_customer_pet")
    dim_pet_with_id.createOrReplaceTempView("dim_customer_pet")
    return dim_pet_with_id


@log_execution
def load_dim_customer(dim_pet_with_id):
    customer_df = mock_df.select(
        "customer_first_name", "customer_last_name", "customer_age", "customer_email",
        "customer_country", "customer_postal_code", "pet_category",
        "customer_pet_type", "customer_pet_name", "customer_pet_breed"
    ).filter(col("customer_email").isNotNull())
    customer_df = deduplicate(customer_df, ["customer_email"], ["customer_age"])
    customer_joined = customer_df.join(
        dim_pet_with_id,
        on=[
            customer_df.customer_pet_type == dim_pet_with_id.customer_pet_type,
            customer_df.customer_pet_name == dim_pet_with_id.customer_pet_name,
            customer_df.customer_pet_breed == dim_pet_with_id.customer_pet_breed
        ],
        how="left"
    ).select(
        customer_df.customer_first_name, customer_df.customer_last_name,
        customer_df.customer_age, customer_df.customer_email,
        customer_df.customer_country, customer_df.customer_postal_code,
        col("customer_pet_id"), customer_df.pet_category
    )
    write_table(customer_joined, "dim_customer")
    dim_customer_with_id = read_table("dim_customer")
    dim_customer_with_id.createOrReplaceTempView("dim_customer")
    return dim_customer_with_id


@log_execution
def load_dim_seller():
    seller_df = mock_df.select(
        "seller_first_name", "seller_last_name", "seller_email",
        "seller_country", "seller_postal_code"
    ).filter(col("seller_email").isNotNull())
    seller_df = deduplicate(seller_df, ["seller_email"], ["seller_last_name"])
    write_table(seller_df, "dim_seller")
    dim_seller_with_id = read_table("dim_seller")
    dim_seller_with_id.createOrReplaceTempView("dim_seller")
    return dim_seller_with_id


@log_execution
def load_dim_supplier():
    supplier_df = mock_df.select(
        "supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
        "supplier_address", "supplier_city", "supplier_country"
    ).filter(col("supplier_name").isNotNull()).distinct()
    write_table(supplier_df, "dim_supplier")
    dim_supplier_with_id = read_table("dim_supplier")
    dim_supplier_with_id.createOrReplaceTempView("dim_supplier")
    return dim_supplier_with_id


@log_execution
def load_dim_product(dim_supplier_with_id):
    product_df = mock_df.select(
        "supplier_name", "supplier_email", "product_name", "product_category", "product_price",
        "product_quantity", "product_weight", "product_color", "product_size", "product_brand",
        "product_material", "product_description", "product_rating", "product_reviews",
        "product_release_date", "product_expiry_date"
    ).filter(col("product_name").isNotNull())
    product_df = deduplicate(
        product_df,
        ["product_name", "product_price", "product_color", "product_size", "product_brand", "product_material"],
        ["product_name", "product_price", "product_color"]
    )
    product_joined = product_df.join(
        dim_supplier_with_id,
        on=[
            product_df.supplier_name == dim_supplier_with_id.supplier_name,
            product_df.supplier_email == dim_supplier_with_id.supplier_email
        ],
        how="left"
    ).select(
        col("product_supplier_id"),
        product_df.product_name, product_df.product_category, product_df.product_price,
        product_df.product_quantity, product_df.product_weight, product_df.product_color,
        product_df.product_size, product_df.product_brand, product_df.product_material,
        product_df.product_description, product_df.product_rating, product_df.product_reviews,
        product_df.product_release_date, product_df.product_expiry_date
    )
    write_table(product_joined, "dim_product")
    dim_product_with_id = read_table("dim_product")
    dim_product_with_id.createOrReplaceTempView("dim_product")
    return dim_product_with_id


@log_execution
def load_dim_store():
    store_df = mock_df.select(
        "store_name", "store_location", "store_city", "store_state",
        "store_country", "store_phone", "store_email"
    ).filter(col("store_name").isNotNull())
    store_df = deduplicate(store_df, ["store_email"], ["store_name"])
    write_table(store_df, "dim_store")
    dim_store_with_id = read_table("dim_store")
    dim_store_with_id.createOrReplaceTempView("dim_store")
    return dim_store_with_id


@log_execution
def load_fact_sales(dim_product_with_id, dim_seller_with_id, dim_customer_with_id, dim_store_with_id):
    fact_df = mock_df.select(
        "product_name", "product_price", "product_color", "product_size",
        "product_brand", "product_material", "seller_email", "customer_email",
        "store_name", "store_email", "sale_quantity", "sale_total_price", "sale_date"
    ).filter(col("sale_date").isNotNull())
    date1 = try_to_date(col("sale_date"), "M/dd/yyyy")
    date2 = try_to_date(col("sale_date"), "MM/dd/yyyy")
    fact_df = fact_df.withColumn("sale_date", coalesce(date1, date2))
    fact_df = fact_df.join(
        dim_product_with_id,
        on=[
            fact_df.product_name == dim_product_with_id.product_name,
            fact_df.product_price == dim_product_with_id.product_price,
            fact_df.product_color == dim_product_with_id.product_color,
            fact_df.product_size == dim_product_with_id.product_size,
            fact_df.product_brand == dim_product_with_id.product_brand,
            fact_df.product_material == dim_product_with_id.product_material
        ],
        how="left"
    )
    fact_df = fact_df.join(
        dim_seller_with_id,
        on=fact_df.seller_email == dim_seller_with_id.seller_email,
        how="left"
    )
    fact_df = fact_df.join(
        dim_customer_with_id,
        on=fact_df.customer_email == dim_customer_with_id.customer_email,
        how="left"
    )
    fact_df = fact_df.join(
        dim_store_with_id,
        on=[fact_df.store_name == dim_store_with_id.store_name,
            fact_df.store_email == dim_store_with_id.store_email],
        how="left"
    )
    fact_final = fact_df.select(
        col("sale_product_id"),
        col("sale_seller_id"),
        col("sale_customer_id"),
        col("sale_store_id"),
        col("sale_quantity"),
        col("sale_total_price"),
        col("sale_date")
    )
    fact_final = fact_final.dropDuplicates()
    write_table(fact_final, "fact_sales")


def main():
    dim_pet_with_id = load_dim_customer_pet()
    dim_customer_with_id = load_dim_customer(dim_pet_with_id)
    dim_seller_with_id = load_dim_seller()
    dim_supplier_with_id = load_dim_supplier()
    dim_product_with_id = load_dim_product(dim_supplier_with_id)
    dim_store_with_id = load_dim_store()
    load_fact_sales(dim_product_with_id, dim_seller_with_id, dim_customer_with_id, dim_store_with_id)

    highest_rated_products(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    lowest_rated_products(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    rating_sales_correlation(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    most_reviewed_products(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)

    top_10_products(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    revenue_by_category(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    product_reviews(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)

    top_10_customers(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    customers_by_country(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    avg_check_per_customer(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)

    revenue_comparsion(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    trends(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    monthly_avg_order_value(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)

    top_stores_by_revenue(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    sales_by_city(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    sales_by_country(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    avg_check_per_store(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)

    top_suppliers_by_revenue(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    avg_price_by_supplier(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)
    sales_by_supplier_country(spark_app, PG_URL, PG_PROPS, CH_URL, CH_PROPS)


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

    spark_app = SparkSession.builder \
        .appName("LoadDataWarehouse") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark_app.sparkContext.setLogLevel("WARN")

    mock_df = spark_app.read.jdbc(url=PG_URL, table="mock_data", properties=PG_PROPS)

    main()
    spark_app.stop()
