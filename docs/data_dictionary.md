# Data Dictionary

## Bronze Layer

### clickstream
Raw clickstream events ingested from Kafka.

| Column | Type | Description |
|--------|------|-------------|
| event_id | string | Unique event identifier |
| session_id | string | Browser session identifier |
| user_id | integer | Authenticated user (nullable) |
| anonymous_id | string | Anonymous visitor ID |
| event_type | string | page_view, click, add_to_cart, purchase, search, wishlist_add |
| page_type | string | home, category, product, cart, checkout, confirmation |
| device | string | desktop, mobile, tablet |
| timestamp | string | ISO 8601 event timestamp |

### orders
Raw order records ingested from Kafka.

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | UUID order identifier |
| user_id | integer | Customer user ID |
| status | string | pending, confirmed, shipped, delivered, cancelled, returned |
| total_amt | double | Final order total including tax and shipping |
| items | array | Line items with product_id, quantity, unit_price |

## Silver Layer

### clickstream
Deduplicated and typed clickstream events.

- `event_ts`: Parsed timestamp (TimestampType)
- `device`: Lowercased
- Duplicates removed on `event_id`

### orders
Cleaned orders with proper types and deduplication.

- Timestamps cast from strings
- Negative totals filtered
- Status lowercased and trimmed

### sessions
Session-level aggregations built from silver clickstream.

| Column | Type | Description |
|--------|------|-------------|
| session_id | string | Session identifier |
| session_start | timestamp | First event in session |
| session_end | timestamp | Last event in session |
| session_duration_min | double | Minutes between first and last event |
| event_count | integer | Total events in session |
| add_to_cart_count | integer | Add-to-cart events |
| converted | boolean | Session resulted in purchase |

## Gold Layer

### fct_daily_revenue
Aggregated daily revenue from completed orders.

| Column | Type | Description |
|--------|------|-------------|
| order_date | date | Calendar date |
| order_count | integer | Completed orders |
| gross_revenue | numeric | Sum of total_amt |
| avg_order_value | numeric | Average order total |
| unique_customers | integer | Distinct user_ids |

### fct_conversion_funnel
Daily session conversion metrics.

| Column | Type | Description |
|--------|------|-------------|
| date_partition | date | Calendar date |
| total_sessions | integer | All sessions |
| sessions_with_cart | integer | Sessions with ≥1 add-to-cart |
| converted_sessions | integer | Sessions with purchase |
| conversion_rate_pct | numeric | converted / total * 100 |

### dim_customers
Customer dimension with lifetime metrics and RFM.

| Column | Type | Description |
|--------|------|-------------|
| user_id | integer | Customer identifier |
| customer_tier | string | platinum/gold/silver/bronze by LTV |
| lifetime_value | numeric | Sum of completed order totals |
| rfm_status | string | active/at_risk/churned/never_purchased |
