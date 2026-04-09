-- =========================
-- 1. PETS
-- =========================
INSERT INTO dim_customer_pet (customer_pet_type, customer_pet_name, customer_pet_breed)
SELECT DISTINCT
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM mock_data
ON CONFLICT DO NOTHING;

-- =========================
-- 2. CUSTOMERS
-- =========================
INSERT INTO dim_customer (
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_id,
    pet_category
)
SELECT DISTINCT ON (m.customer_email)
    m.customer_first_name,
    m.customer_last_name,
    m.customer_age,
    m.customer_email,
    m.customer_country,
    m.customer_postal_code,
    p.customer_pet_id,
    m.pet_category
FROM mock_data m
JOIN dim_customer_pet p
    ON m.customer_pet_type = p.customer_pet_type
   AND m.customer_pet_name = p.customer_pet_name
   AND m.customer_pet_breed = p.customer_pet_breed
WHERE m.customer_email IS NOT NULL
ORDER BY m.customer_email, m.customer_age
ON CONFLICT DO NOTHING;

-- =========================
-- 3. SELLERS
-- =========================
INSERT INTO dim_seller (
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
)
SELECT DISTINCT ON (m.seller_email)
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data m
WHERE m.seller_email IS NOT NULL
ORDER BY m.seller_email, m.seller_last_name
ON CONFLICT DO NOTHING;

-- =========================
-- 4. SUPPLIERS
-- =========================
INSERT INTO dim_supplier (
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
)
SELECT DISTINCT
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data
WHERE supplier_name IS NOT NULL
ON CONFLICT DO NOTHING;

-- =========================
-- 5. PRODUCTS
-- =========================
INSERT INTO dim_product (
    product_supplier_id,
    product_name,
    product_category,
    product_price,
    product_quantity,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
)
SELECT DISTINCT ON (
    m.product_name,
    m.product_price,
    m.product_color,
    m.product_size,
    m.product_brand,
    m.product_material
)
    s.product_supplier_id,
    m.product_name,
    m.product_category,
    m.product_price,
    m.product_quantity,
    m.product_weight,
    m.product_color,
    m.product_size,
    m.product_brand,
    m.product_material,
    m.product_description,
    m.product_rating,
    m.product_reviews,
    m.product_release_date,
    m.product_expiry_date
FROM mock_data m
JOIN dim_supplier s
    ON m.supplier_name = s.supplier_name
   AND m.supplier_email = s.supplier_email
WHERE m.product_name IS NOT NULL
ORDER BY m.product_name, m.product_price, m.product_color
ON CONFLICT DO NOTHING;

-- =========================
-- 6. STORES
-- =========================
INSERT INTO dim_store (
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
)
SELECT DISTINCT ON (m.store_email)
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM mock_data m
WHERE store_name IS NOT NULL
ON CONFLICT DO NOTHING;

-- =========================
-- ИНДЕКСЫ
-- =========================
CREATE INDEX IF NOT EXISTS idx_product_name ON dim_product(product_name);
CREATE INDEX IF NOT EXISTS idx_product_price ON dim_product(product_price);
CREATE INDEX IF NOT EXISTS idx_seller_email ON dim_seller(seller_email);
CREATE INDEX IF NOT EXISTS idx_customer_email ON dim_customer(customer_email);
CREATE INDEX IF NOT EXISTS idx_store_name ON dim_store(store_name);
CREATE INDEX IF NOT EXISTS idx_store_email ON dim_store(store_email);

-- =========================
-- 7. FACT TABLE
-- =========================
INSERT INTO fact_sales (
    sale_product_id,
    sale_seller_id,
    sale_customer_id,
    sale_store_id,
    sale_quantity,
    sale_total_price,
    sale_date
)
SELECT
    p.sale_product_id,
    s.sale_seller_id,
    c.sale_customer_id,
    st.sale_store_id,
    m.sale_quantity,
    m.sale_total_price,
    TO_DATE(m.sale_date, 'MM/DD/YYYY')
FROM mock_data m
JOIN dim_product p
    ON m.product_name = p.product_name
    AND p.product_price = m.product_price
    AND m.product_color = p.product_color
    AND m.product_size = p.product_size
    AND m.product_brand = p.product_brand
    AND m.product_material = p.product_material
JOIN dim_seller s
    ON m.seller_email = s.seller_email
JOIN dim_customer c
    ON m.customer_email = c.customer_email
JOIN dim_store st
    ON m.store_name = st.store_name
    AND st.store_email = m.store_email
WHERE m.sale_date IS NOT NULL;