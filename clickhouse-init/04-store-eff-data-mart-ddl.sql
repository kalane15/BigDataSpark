CREATE TABLE IF NOT EXISTS dm4_top_stores_by_revenue (
    store_name      String,
    total_revenue   Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY total_revenue;

CREATE TABLE IF NOT EXISTS dm4_sales_by_city (
    city           String,
    total_sales    Decimal(15,2),
    total_quantity UInt64
) ENGINE = MergeTree()
ORDER BY city;

CREATE TABLE IF NOT EXISTS dm4_sales_by_country (
    country        String,
    total_sales    Decimal(15,2),
    total_quantity UInt64
) ENGINE = MergeTree()
ORDER BY country;

CREATE TABLE IF NOT EXISTS dm4_avg_check_by_store (
    store_name       String,
    avg_check_amount Decimal(15,2),
    total_orders     UInt64
) ENGINE = MergeTree()
ORDER BY store_name;