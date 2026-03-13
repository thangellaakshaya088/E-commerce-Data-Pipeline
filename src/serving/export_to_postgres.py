import os
import logging
import psycopg2
import pandas as pd
from src.serving.duckdb_queries import get_connection, query_daily_revenue, query_conversion_funnel, query_top_customers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB = os.getenv("POSTGRES_DB", "shopstream")
PG_USER = os.getenv("POSTGRES_USER", "shopstream")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "shopstream123")


def get_pg_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)


def upsert_dataframe(df: pd.DataFrame, table: str, pk_cols: list[str]) -> None:
    conn = get_pg_conn()
    cur = conn.cursor()
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_str = ", ".join(cols)
    update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c not in pk_cols])
    conflict_str = ", ".join(pk_cols)

    sql = f"""
        INSERT INTO {table} ({col_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}
    """
    rows = [tuple(row) for row in df.itertuples(index=False)]
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Upserted {len(rows)} rows into {table}")


def export_gold_tables() -> None:
    daily_rev = query_daily_revenue(90)
    upsert_dataframe(daily_rev, "gold_daily_revenue", ["order_date"])

    funnel = query_conversion_funnel(30)
    upsert_dataframe(funnel, "gold_conversion_funnel", ["date_partition"])

    customers = query_top_customers(1000)
    upsert_dataframe(customers, "gold_customers", ["user_id"])


if __name__ == "__main__":
    export_gold_tables()
