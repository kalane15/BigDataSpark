CREATE TABLE IF NOT EXISTS top_10_products (
    product_name    String,
    product_price UInt64,
    total_quantity  UInt64,
    total_revenue   Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY total_revenue;