import json
import time
import logging
import argparse
import random
from kafka import KafkaProducer
from .config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_CLICKSTREAM, KAFKA_TOPIC_ORDERS, KAFKA_TOPIC_USERS, BATCH_SIZE
from .clickstream_generator import generate_event, generate_session_events
from .orders_generator import generate_order
from .users_generator import generate_user

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def publish_clickstream(producer: KafkaProducer, n: int = BATCH_SIZE) -> None:
    sent = 0
    for _ in range(n // 5):
        for event in generate_session_events(random.randint(3, 8)):
            producer.send(KAFKA_TOPIC_CLICKSTREAM, key=event["session_id"], value=event)
            sent += 1
    producer.flush()
    logger.info(f"Published {sent} clickstream events")


def publish_orders(producer: KafkaProducer, n: int = 20) -> None:
    for _ in range(n):
        order = generate_order()
        producer.send(KAFKA_TOPIC_ORDERS, key=order["order_id"], value=order)
    producer.flush()
    logger.info(f"Published {n} orders")


def publish_users(producer: KafkaProducer, n: int = 10) -> None:
    for i in range(n):
        user = generate_user()
        producer.send(KAFKA_TOPIC_USERS, key=str(user["user_id"]), value=user)
    producer.flush()
    logger.info(f"Published {n} users")


def run_continuous(interval_seconds: float = 2.0) -> None:
    producer = make_producer()
    logger.info("Starting continuous data generation...")
    try:
        while True:
            publish_clickstream(producer, n=BATCH_SIZE)
            if random.random() > 0.7:
                publish_orders(producer, n=random.randint(5, 20))
            if random.random() > 0.9:
                publish_users(producer, n=random.randint(2, 10))
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Stopping producer")
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["continuous", "batch"], default="continuous")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()

    if args.mode == "continuous":
        run_continuous(args.interval)
    else:
        p = make_producer()
        publish_clickstream(p)
        publish_orders(p)
        publish_users(p)
        p.close()
