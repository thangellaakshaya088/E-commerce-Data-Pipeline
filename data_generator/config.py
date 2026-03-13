import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_CLICKSTREAM = os.getenv("KAFKA_TOPIC_CLICKSTREAM", "clickstream")
KAFKA_TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "orders")
KAFKA_TOPIC_USERS = os.getenv("KAFKA_TOPIC_USERS", "users")

PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Books", "Home & Garden",
    "Sports", "Toys", "Beauty", "Automotive", "Food", "Jewelry"
]

PAGE_TYPES = ["home", "category", "product", "cart", "checkout", "confirmation", "search", "profile"]
EVENT_TYPES = ["page_view", "click", "add_to_cart", "remove_from_cart", "purchase", "search", "wishlist_add"]

NUM_USERS = 10000
NUM_PRODUCTS = 5000
BATCH_SIZE = 100
