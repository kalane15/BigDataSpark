CREATE TABLE IF NOT EXISTS dm3_revenue_comparsion (
    year             UInt16,
    month            UInt8,
    total_revenue    Decimal(15,2),
    total_orders     UInt64,
    avg_order_value  Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS dm3_trends (
    year                 UInt16,
    total_revenue        Decimal(15,2),
    revenue_change_percent Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY year;

CREATE TABLE IF NOT EXISTS dm3_monthly_avg_order_value (
    year             UInt16,
    month            UInt8,
    avg_order_value  Decimal(15,2)
) ENGINE = MergeTree()
ORDER BY (year, month);