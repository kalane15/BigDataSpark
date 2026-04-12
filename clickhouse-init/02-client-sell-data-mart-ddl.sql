CREATE TABLE IF NOT EXISTS dm2_top_10_customers_by_total (
    customer_email    String,
    customer_name     String,
    total_spent       Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY total_spent;

CREATE TABLE IF NOT EXISTS dm2_customers_by_country (
    country_name      String,
    customer_count    UInt64
) ENGINE = MergeTree()
ORDER BY country_name;

CREATE TABLE IF NOT EXISTS dm2_avg_check_per_customer (
    customer_email    String,
    customer_name     String,
    avg_check         Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY avg_check;