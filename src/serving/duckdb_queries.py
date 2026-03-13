import os
import duckdb
import pandas as pd

BUCKET = os.getenv("MINIO_BUCKET", "shopstream")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def get_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT.replace("http://", "")}';
        SET s3_access_key_id='{ACCESS_KEY}';
        SET s3_secret_access_key='{SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con


def query_daily_revenue(days: int = 30) -> pd.DataFrame:
    con = get_connection()
    return con.execute(f"""
        SELECT
            order_date,
            order_count,
            gross_revenue,
            avg_order_value,
            unique_customers,
            ROUND(gross_revenue / LAG(gross_revenue) OVER (ORDER BY order_date) - 1, 4) AS revenue_growth
        FROM delta_scan('s3://{BUCKET}/gold/fct_daily_revenue')
        WHERE order_date >= CURRENT_DATE - INTERVAL '{days} days'
        ORDER BY order_date DESC
    """).df()


def query_conversion_funnel(days: int = 7) -> pd.DataFrame:
    con = get_connection()
    return con.execute(f"""
        SELECT
            date_partition,
            total_sessions,
            sessions_with_cart,
            converted_sessions,
            ROUND(cart_rate * 100, 2) AS cart_rate_pct,
            ROUND(conversion_rate * 100, 2) AS conversion_rate_pct,
            ROUND(avg_session_min, 1) AS avg_session_min
        FROM delta_scan('s3://{BUCKET}/gold/fct_conversion_funnel')
        WHERE date_partition >= CURRENT_DATE - INTERVAL '{days} days'
        ORDER BY date_partition DESC
    """).df()


def query_top_customers(limit: int = 20) -> pd.DataFrame:
    con = get_connection()
    return con.execute(f"""
        SELECT
            user_id,
            email,
            customer_tier,
            lifetime_orders,
            ROUND(lifetime_value, 2) AS lifetime_value,
            ROUND(avg_order_value, 2) AS avg_order_value,
            segment
        FROM delta_scan('s3://{BUCKET}/gold/dim_customers')
        WHERE lifetime_orders > 0
        ORDER BY lifetime_value DESC
        LIMIT {limit}
    """).df()


def query_revenue_by_status() -> pd.DataFrame:
    con = get_connection()
    return con.execute(f"""
        SELECT
            status,
            COUNT(*) AS order_count,
            ROUND(SUM(total_amt), 2) AS total_revenue,
            ROUND(AVG(total_amt), 2) AS avg_order_value
        FROM delta_scan('s3://{BUCKET}/silver/orders')
        GROUP BY status
        ORDER BY total_revenue DESC
    """).df()


if __name__ == "__main__":
    print("=== Daily Revenue (Last 30 Days) ===")
    print(query_daily_revenue(30).to_string())
    print("\n=== Conversion Funnel (Last 7 Days) ===")
    print(query_conversion_funnel(7).to_string())
    print("\n=== Top 20 Customers ===")
    print(query_top_customers(20).to_string())
