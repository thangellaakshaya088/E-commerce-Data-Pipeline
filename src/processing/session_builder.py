import os
import logging
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from src.processing.utils.spark_session import get_spark_session
from src.processing.utils.delta_helpers import write_delta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET", "shopstream")
SESSION_TIMEOUT_MINUTES = 30


def build_sessions(spark) -> None:
    silver_path = f"s3a://{BUCKET}/silver/clickstream"
    sessions_path = f"s3a://{BUCKET}/silver/sessions"

    df = spark.read.format("delta").load(silver_path)

    w_session = Window.partitionBy("session_id").orderBy("event_ts")
    w_session_unbounded = Window.partitionBy("session_id").orderBy("event_ts").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    sessions = (
        df.withColumn("prev_event_ts", F.lag("event_ts").over(w_session))
        .withColumn(
            "time_since_prev_min",
            (F.col("event_ts").cast("long") - F.col("prev_event_ts").cast("long")) / 60,
        )
        .groupBy("session_id", "user_id", "device", "browser", "os")
        .agg(
            F.min("event_ts").alias("session_start"),
            F.max("event_ts").alias("session_end"),
            F.count("event_id").alias("event_count"),
            F.countDistinct("page_type").alias("unique_pages"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.collect_set("page_type").alias("pages_visited"),
            F.first("date_partition").alias("date_partition"),
        )
        .withColumn(
            "session_duration_min",
            (F.col("session_end").cast("long") - F.col("session_start").cast("long")) / 60,
        )
        .withColumn("converted", F.col("purchase_count") > 0)
    )

    write_delta(sessions, sessions_path, mode="overwrite", partition_by=["date_partition"])
    logger.info(f"Sessions built: {sessions.count()} sessions")


if __name__ == "__main__":
    spark = get_spark_session("SessionBuilder")
    build_sessions(spark)
    spark.stop()
