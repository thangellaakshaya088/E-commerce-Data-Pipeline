import random
import uuid
from datetime import datetime
from faker import Faker
from .config import PAGE_TYPES, EVENT_TYPES, NUM_USERS, NUM_PRODUCTS

fake = Faker()

DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
OS_LIST = ["Windows", "macOS", "Android", "iOS", "Linux"]


def generate_event(user_id: int | None = None, session_id: str | None = None) -> dict:
    uid = user_id or random.randint(1, NUM_USERS)
    sid = session_id or str(uuid.uuid4())
    event_type = random.choice(EVENT_TYPES)
    page = random.choice(PAGE_TYPES)

    event = {
        "event_id": str(uuid.uuid4()),
        "session_id": sid,
        "user_id": uid if random.random() > 0.15 else None,
        "anonymous_id": str(uuid.uuid4()),
        "event_type": event_type,
        "page_type": page,
        "url": f"https://shopstream.com/{page}/{fake.slug()}",
        "referrer": fake.url() if random.random() > 0.4 else None,
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "os": random.choice(OS_LIST),
        "ip_address": fake.ipv4(),
        "timestamp": datetime.utcnow().isoformat(),
        "properties": {},
    }

    if event_type in ("add_to_cart", "purchase", "wishlist_add"):
        event["properties"]["product_id"] = random.randint(1, NUM_PRODUCTS)
        event["properties"]["quantity"] = random.randint(1, 5)
        event["properties"]["price"] = round(random.uniform(5.99, 999.99), 2)

    if event_type == "search":
        event["properties"]["query"] = fake.catch_phrase()
        event["properties"]["results_count"] = random.randint(0, 200)

    return event


def generate_session_events(session_length: int = 5) -> list[dict]:
    uid = random.randint(1, NUM_USERS)
    sid = str(uuid.uuid4())
    return [generate_event(uid, sid) for _ in range(session_length)]
