CREATE TABLE IF NOT EXISTS dm5_top_suppliers_by_revenue (
    supplier_name   String,
    total_revenue   Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY total_revenue;

CREATE TABLE IF NOT EXISTS dm5_avg_price_by_supplier (
    supplier_name     String,
    avg_product_price Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY supplier_name;

CREATE TABLE IF NOT EXISTS dm5_sales_by_supplier_country (
    country         String,
    total_sales     Decimal(15,2),
    total_quantity  UInt64
) ENGINE = MergeTree()
ORDER BY country;