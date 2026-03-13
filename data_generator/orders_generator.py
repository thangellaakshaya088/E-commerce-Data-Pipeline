import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from .config import NUM_USERS, NUM_PRODUCTS

fake = Faker()

STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
CARRIERS = ["FedEx", "UPS", "USPS", "DHL"]


def generate_order_item(product_id: int | None = None) -> dict:
    pid = product_id or random.randint(1, NUM_PRODUCTS)
    unit_price = round(random.uniform(5.99, 499.99), 2)
    qty = random.randint(1, 5)
    return {
        "product_id": pid,
        "quantity": qty,
        "unit_price": unit_price,
        "discount_amt": round(random.uniform(0, unit_price * 0.3), 2),
        "line_total": round(unit_price * qty, 2),
    }


def generate_order(user_id: int | None = None) -> dict:
    uid = user_id or random.randint(1, NUM_USERS)
    num_items = random.randint(1, 6)
    items = [generate_order_item() for _ in range(num_items)]
    subtotal = sum(i["line_total"] for i in items)
    shipping = round(random.choice([0, 4.99, 9.99, 14.99]), 2)
    tax = round(subtotal * 0.08, 2)
    created_at = fake.date_time_between(start_date="-1y", end_date="now")

    status = random.choices(STATUSES, weights=[5, 10, 20, 55, 7, 3])[0]

    order = {
        "order_id": str(uuid.uuid4()),
        "user_id": uid,
        "status": status,
        "items": items,
        "item_count": num_items,
        "subtotal": round(subtotal, 2),
        "shipping_cost": shipping,
        "tax_amt": tax,
        "total_amt": round(subtotal + shipping + tax, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "shipping_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip": fake.zipcode(),
            "country": "US",
        },
        "created_at": created_at.isoformat(),
        "updated_at": (created_at + timedelta(hours=random.randint(1, 72))).isoformat(),
    }

    if status in ("shipped", "delivered"):
        order["carrier"] = random.choice(CARRIERS)
        order["tracking_number"] = fake.bothify("1Z###?##########")
        order["shipped_at"] = (created_at + timedelta(days=random.randint(1, 3))).isoformat()

    if status == "delivered":
        order["delivered_at"] = (created_at + timedelta(days=random.randint(3, 10))).isoformat()

    return order


def generate_orders_batch(n: int = 50) -> list[dict]:
    return [generate_order() for _ in range(n)]
