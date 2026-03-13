import random
import uuid
from faker import Faker
from .config import PRODUCT_CATEGORIES

fake = Faker()

BRANDS = ["TechPro", "StyleHub", "HomeEssentials", "SportMax", "LuxeGoods", "ValuePlus", "EcoChoice", "PremiumCo"]


def generate_product(product_id: int | None = None) -> dict:
    category = random.choice(PRODUCT_CATEGORIES)
    base_price = round(random.uniform(5.99, 999.99), 2)
    discount = round(random.uniform(0, 0.4), 2)
    return {
        "product_id": product_id or random.randint(1, 5000),
        "sku": f"SKU-{fake.bothify('??##??##').upper()}",
        "name": f"{random.choice(BRANDS)} {fake.catch_phrase()[:40]}",
        "category": category,
        "subcategory": fake.word().title(),
        "brand": random.choice(BRANDS),
        "price": base_price,
        "discount_pct": discount,
        "final_price": round(base_price * (1 - discount), 2),
        "stock_qty": random.randint(0, 500),
        "rating": round(random.uniform(1.0, 5.0), 1),
        "review_count": random.randint(0, 5000),
        "is_active": random.random() > 0.1,
        "created_at": fake.date_time_between(start_date="-2y", end_date="-6m").isoformat(),
        "updated_at": fake.date_time_between(start_date="-6m", end_date="now").isoformat(),
        "weight_kg": round(random.uniform(0.1, 20.0), 2),
        "tags": random.sample(["sale", "new", "trending", "bestseller", "clearance", "exclusive"], k=random.randint(0, 3)),
    }


def generate_products_batch(n: int = 100) -> list[dict]:
    return [generate_product(i + 1) for i in range(n)]
