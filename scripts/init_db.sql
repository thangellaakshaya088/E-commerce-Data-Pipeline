CREATE TABLE IF NOT EXISTS users (
    user_id           INTEGER PRIMARY KEY,
    email             VARCHAR(255) UNIQUE NOT NULL,
    username          VARCHAR(100),
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    phone             VARCHAR(50),
    is_active         BOOLEAN DEFAULT true,
    segment           VARCHAR(50),
    acquisition_channel VARCHAR(50),
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id        INTEGER PRIMARY KEY,
    sku               VARCHAR(50) UNIQUE,
    name              VARCHAR(255),
    category          VARCHAR(100),
    subcategory       VARCHAR(100),
    brand             VARCHAR(100),
    price             NUMERIC(10,2),
    discount_pct      NUMERIC(5,4),
    final_price       NUMERIC(10,2),
    stock_qty         INTEGER,
    rating            NUMERIC(3,1),
    review_count      INTEGER,
    is_active         BOOLEAN DEFAULT true,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id          VARCHAR(36) PRIMARY KEY,
    user_id           INTEGER REFERENCES users(user_id),
    status            VARCHAR(50),
    item_count        INTEGER,
    subtotal          NUMERIC(12,2),
    shipping_cost     NUMERIC(10,2),
    tax_amt           NUMERIC(10,2),
    total_amt         NUMERIC(12,2),
    payment_method    VARCHAR(50),
    carrier           VARCHAR(50),
    tracking_number   VARCHAR(100),
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    shipped_at        TIMESTAMP,
    delivered_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold_daily_revenue (
    order_date        DATE PRIMARY KEY,
    order_count       INTEGER,
    gross_revenue     NUMERIC(15,2),
    avg_order_value   NUMERIC(10,2),
    unique_customers  INTEGER,
    revenue_growth    NUMERIC(8,4),
    updated_at        TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_conversion_funnel (
    date_partition        DATE PRIMARY KEY,
    total_sessions        INTEGER,
    sessions_with_cart    INTEGER,
    converted_sessions    INTEGER,
    cart_rate_pct         NUMERIC(6,2),
    conversion_rate_pct   NUMERIC(6,2),
    avg_session_min       NUMERIC(8,2),
    updated_at            TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_customers (
    user_id           INTEGER PRIMARY KEY,
    email             VARCHAR(255),
    customer_tier     VARCHAR(20),
    lifetime_orders   INTEGER,
    lifetime_value    NUMERIC(12,2),
    avg_order_value   NUMERIC(10,2),
    segment           VARCHAR(50),
    updated_at        TIMESTAMP DEFAULT now()
);

CREATE DATABASE IF NOT EXISTS airflow;
