CREATE TABLE IF NOT EXISTS dm6_highest_rated_products (
    product_name   String,
    product_rating Decimal(3,1)
) ENGINE = MergeTree()
ORDER BY product_rating;

CREATE TABLE IF NOT EXISTS dm6_lowest_rated_products (
    product_name   String,
    product_rating Decimal(3,1)
) ENGINE = MergeTree()
ORDER BY product_rating;

CREATE TABLE IF NOT EXISTS dm6_rating_sales_correlation (
    correlation_value Float64
) ENGINE = MergeTree()
ORDER BY correlation_value;

CREATE TABLE IF NOT EXISTS dm6_most_reviewed_products (
    product_name    String,
    product_reviews UInt64,
    product_rating  Decimal(3,1)
) ENGINE = MergeTree()
ORDER BY product_reviews;