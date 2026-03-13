import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from src.processing.utils.spark_session import get_spark_session
from src.processing.utils.schema_definitions import CLICKSTREAM_SCHEMA, ORDER_SCHEMA

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "shopstream")


def ingest_clickstream() -> None:
    spark = get_spark_session("BronzeClickstream")
    bronze_path = f"s3a://{MINIO_BUCKET}/bronze/clickstream"

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "clickstream")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    parsed = (
        raw.select(
            F.col("value").cast(StringType()).alias("raw_json"),
            F.col("offset"),
            F.col("partition"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("payload", F.from_json(F.col("raw_json"), CLICKSTREAM_SCHEMA))
        .select("raw_json", "offset", "partition", "kafka_timestamp", "payload.*")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("date_partition", F.to_date(F.col("kafka_timestamp")))
    )

    query = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/clickstream")
        .partitionBy("date_partition")
        .start(bronze_path)
    )

    logger.info("Clickstream streaming job started")
    query.awaitTermination()


def ingest_orders() -> None:
    spark = get_spark_session("BronzeOrders")
    bronze_path = f"s3a://{MINIO_BUCKET}/bronze/orders"

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.select(F.col("value").cast(StringType()).alias("raw_json"), F.col("timestamp").alias("kafka_timestamp"))
        .withColumn("payload", F.from_json(F.col("raw_json"), ORDER_SCHEMA))
        .select("raw_json", "kafka_timestamp", "payload.*")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("date_partition", F.to_date(F.col("created_at")))
    )

    query = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/orders")
        .partitionBy("date_partition")
        .start(bronze_path)
    )

    logger.info("Orders streaming job started")
    query.awaitTermination()


if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else "clickstream"
    if topic == "orders":
        ingest_orders()
    else:
        ingest_clickstream()
