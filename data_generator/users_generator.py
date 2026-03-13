import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()


def generate_user(user_id: int | None = None) -> dict:
    uid = user_id or random.randint(1, 10000)
    created = fake.date_time_between(start_date="-2y", end_date="now")
    return {
        "user_id": uid,
        "email": fake.email(),
        "username": fake.user_name(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "phone": fake.phone_number(),
        "address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip": fake.zipcode(),
            "country": "US",
        },
        "created_at": created.isoformat(),
        "updated_at": (created + timedelta(days=random.randint(0, 365))).isoformat(),
        "is_active": random.random() > 0.05,
        "segment": random.choice(["new", "returning", "loyal", "at_risk", "churned"]),
        "acquisition_channel": random.choice(["organic", "paid_search", "social", "email", "referral", "direct"]),
    }


def generate_users_batch(n: int = 100) -> list[dict]:
    return [generate_user(i + 1) for i in range(n)]
