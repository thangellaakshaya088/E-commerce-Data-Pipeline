import os
import sys
import json
import psycopg2
import psycopg2.extras
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from data_generator.users_generator import generate_users_batch
from data_generator.products_generator import generate_products_batch
from data_generator.orders_generator import generate_orders_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "shopstream"),
        user=os.getenv("POSTGRES_USER", "shopstream"),
        password=os.getenv("POSTGRES_PASSWORD", "shopstream123"),
    )


def seed_users(conn, n: int = 1000) -> None:
    users = generate_users_batch(n)
    with conn.cursor() as cur:
        for user in users:
            cur.execute(
                """
                INSERT INTO users (user_id, email, username, first_name, last_name, phone,
                    is_active, segment, acquisition_channel, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO NOTHING
                """,
                (
                    user["user_id"], user["email"], user["username"],
                    user["first_name"], user["last_name"], user["phone"],
                    user["is_active"], user["segment"], user["acquisition_channel"],
                    user["created_at"], user["updated_at"],
                ),
            )
    conn.commit()
    logger.info(f"Seeded {n} users")


def seed_products(conn, n: int = 500) -> None:
    products = generate_products_batch(n)
    with conn.cursor() as cur:
        for p in products:
            cur.execute(
                """
                INSERT INTO products (product_id, sku, name, category, subcategory, brand,
                    price, discount_pct, final_price, stock_qty, rating, review_count, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
                """,
                (
                    p["product_id"], p["sku"], p["name"], p["category"], p["subcategory"],
                    p["brand"], p["price"], p["discount_pct"], p["final_price"],
                    p["stock_qty"], p["rating"], p["review_count"], p["is_active"],
                    p["created_at"], p["updated_at"],
                ),
            )
    conn.commit()
    logger.info(f"Seeded {n} products")


def seed_orders(conn, n: int = 2000) -> None:
    orders = generate_orders_batch(n)
    with conn.cursor() as cur:
        for o in orders:
            cur.execute(
                """
                INSERT INTO orders (order_id, user_id, status, item_count, subtotal,
                    shipping_cost, tax_amt, total_amt, payment_method, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
                """,
                (
                    o["order_id"], o["user_id"], o["status"], o["item_count"],
                    o["subtotal"], o["shipping_cost"], o["tax_amt"], o["total_amt"],
                    o["payment_method"], o["created_at"], o["updated_at"],
                ),
            )
    conn.commit()
    logger.info(f"Seeded {n} orders")


if __name__ == "__main__":
    conn = get_conn()
    seed_users(conn)
    seed_products(conn)
    seed_orders(conn)
    conn.close()
    logger.info("Database seeding complete")
