CREATE TABLE IF NOT EXISTS dm1_top_10_products (
    product_name    String,
    product_price UInt64,
    total_quantity  UInt64,
    total_revenue   Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY total_revenue;

CREATE TABLE IF NOT EXISTS dm1_revenue_by_category (
    category_name  String,
    total_revenue  Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY total_revenue;

CREATE TABLE IF NOT EXISTS dm1_product_rating_reviews (
    product_name   String,
    avg_rating     Decimal(3,1),
    total_reviews  UInt64
) ENGINE = MergeTree()
ORDER BY avg_rating;