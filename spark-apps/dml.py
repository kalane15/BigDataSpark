# spark-apps/load_data_warehouse.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, try_to_date
import os

# ============================================
# Параметры подключения к PostgreSQL
# ============================================
PG_URL = "jdbc:postgresql://postgres:5432/postgres"
PG_PROPS = {
    "user": "postgres",
    "password": "mysecretpassword",
    "driver": "org.postgresql.Driver"
}


# ============================================
# Функция для записи DataFrame в таблицу (append)
# ============================================
def write_table(df, table_name):
    df.write.mode("append") \
        .jdbc(url=PG_URL, table=table_name, properties=PG_PROPS)


# ============================================
# Функция для чтения таблицы целиком
# ============================================
def read_table(table_name):
    """Читает всю таблицу из PostgreSQL."""
    return spark.read.jdbc(url=PG_URL, table=table_name, properties=PG_PROPS)


# ============================================
# Основной процесс
# ============================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("LoadDataWarehouse") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    mock_df = spark.read.jdbc(url=PG_URL, table="mock_data", properties=PG_PROPS)

    # ========================================
    # DIM_CUSTOMER_PET
    # ========================================
    print("Загрузка dim_customer_pet...")
    dim_pet = mock_df.select(
        "customer_pet_type", "customer_pet_name", "customer_pet_breed"
    ).distinct().filter(col("customer_pet_type").isNotNull())
    write_table(dim_pet, "dim_customer_pet")
    # Читаем обратно, чтобы получить сгенерированные ID
    dim_pet_with_id = read_table("dim_customer_pet")
    dim_pet_with_id.createOrReplaceTempView("dim_customer_pet")

    # ========================================
    # DIM_CUSTOMER
    # ========================================
    print("Загрузка dim_customer...")
    # Аналог SELECT DISTINCT ON с сортировкой: группируем по email, берем первую строку по customer_age
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    window_cust = Window.partitionBy("customer_email").orderBy("customer_age")
    customer_df = mock_df.select(
        "customer_first_name", "customer_last_name", "customer_age", "customer_email",
        "customer_country", "customer_postal_code", "pet_category",
        "customer_pet_type", "customer_pet_name", "customer_pet_breed"
    ).filter(col("customer_email").isNotNull())
    customer_df = customer_df.withColumn("rn", row_number().over(window_cust)).filter(col("rn") == 1).drop("rn")
    # Присоединяем pet_id
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

    # ========================================
    # DIM_SELLER
    # ========================================
    print("Загрузка dim_seller...")
    window_seller = Window.partitionBy("seller_email").orderBy("seller_last_name")
    seller_df = mock_df.select(
        "seller_first_name", "seller_last_name", "seller_email",
        "seller_country", "seller_postal_code"
    ).filter(col("seller_email").isNotNull())
    seller_df = seller_df.withColumn("rn", row_number().over(window_seller)).filter(col("rn") == 1).drop("rn")
    write_table(seller_df, "dim_seller")
    dim_seller_with_id = read_table("dim_seller")
    dim_seller_with_id.createOrReplaceTempView("dim_seller")

    # ========================================
    # DIM_SUPPLIER
    # ========================================
    print("Загрузка dim_supplier...")
    supplier_df = mock_df.select(
        "supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
        "supplier_address", "supplier_city", "supplier_country"
    ).filter(col("supplier_name").isNotNull()).distinct()
    write_table(supplier_df, "dim_supplier")
    dim_supplier_with_id = read_table("dim_supplier")
    dim_supplier_with_id.createOrReplaceTempView("dim_supplier")

    # ========================================
    # DIM_PRODUCT
    # ========================================
    print("Загрузка dim_product...")
    window_prod = Window.partitionBy(
        "product_name", "product_price", "product_color", "product_size", "product_brand", "product_material"
    ).orderBy("product_name", "product_price", "product_color")
    product_df = mock_df.select(
        "supplier_name", "supplier_email", "product_name", "product_category", "product_price",
        "product_quantity", "product_weight", "product_color", "product_size", "product_brand",
        "product_material", "product_description", "product_rating", "product_reviews",
        "product_release_date", "product_expiry_date"
    ).filter(col("product_name").isNotNull())
    product_df = product_df.withColumn("rn", row_number().over(window_prod)).filter(col("rn") == 1).drop("rn")
    # Присоединяем supplier_id
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

    # ========================================
    # DIM_STORE
    # ========================================
    print("Загрузка dim_store...")
    window_store = Window.partitionBy("store_email").orderBy("store_name")
    store_df = mock_df.select(
        "store_name", "store_location", "store_city", "store_state",
        "store_country", "store_phone", "store_email"
    ).filter(col("store_name").isNotNull())
    store_df = store_df.withColumn("rn", row_number().over(window_store)).filter(col("rn") == 1).drop("rn")
    write_table(store_df, "dim_store")
    dim_store_with_id = read_table("dim_store")
    dim_store_with_id.createOrReplaceTempView("dim_store")

    # ========================================
    # FACT_SALES
    # ========================================
    print("Загрузка fact_sales...")
    # Присоединяем все внешние ключи
    fact_df = mock_df.select(
        "product_name", "product_price", "product_color", "product_size",
        "product_brand", "product_material", "seller_email", "customer_email",
        "store_name", "store_email", "sale_quantity", "sale_total_price", "sale_date"
    ).filter(col("sale_date").isNotNull())
    # Преобразуем дату
    date1 = try_to_date(col("sale_date"), "M/dd/yyyy")
    date2 = try_to_date(col("sale_date"), "MM/dd/yyyy")
    fact_df = fact_df.withColumn("sale_date", coalesce(date1, date2))
    # Присоединяем product
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
    # Присоединяем seller
    fact_df = fact_df.join(
        dim_seller_with_id,
        on=fact_df.seller_email == dim_seller_with_id.seller_email,
        how="left"
    )
    # Присоединяем customer
    fact_df = fact_df.join(
        dim_customer_with_id,
        on=fact_df.customer_email == dim_customer_with_id.customer_email,
        how="left"
    )
    # Присоединяем store
    fact_df = fact_df.join(
        dim_store_with_id,
        on=[fact_df.store_name == dim_store_with_id.store_name,
            fact_df.store_email == dim_store_with_id.store_email],
        how="left"
    )
    # Выбираем финальные колонки
    fact_final = fact_df.select(
        col("sale_product_id"),
        col("sale_seller_id"),
        col("sale_customer_id"),
        col("sale_store_id"),
        col("sale_quantity"),
        col("sale_total_price"),
        col("sale_date")
    )
    # Удаляем возможные дубликаты (если JOIN породил дубли)
    fact_final = fact_final.dropDuplicates()
    write_table(fact_final, "fact_sales")

    print("Загрузка данных завершена успешно.")
    spark.stop()
