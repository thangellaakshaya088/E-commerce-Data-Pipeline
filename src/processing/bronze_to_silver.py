import os
import logging
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from src.processing.utils.spark_session import get_spark_session
from src.processing.utils.delta_helpers import write_delta, upsert_delta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET", "shopstream")


def clean_clickstream(spark) -> None:
    bronze_path = f"s3a://{BUCKET}/bronze/clickstream"
    silver_path = f"s3a://{BUCKET}/silver/clickstream"

    df = spark.read.format("delta").load(bronze_path)

    df = (
        df.dropDuplicates(["event_id"])
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_type").isNotNull())
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("user_id", F.col("user_id").cast("integer"))
        .withColumn("device", F.lower(F.trim(F.col("device"))))
        .withColumn("browser", F.trim(F.col("browser")))
        .withColumn("os", F.trim(F.col("os")))
        .withColumn("date_partition", F.to_date("event_ts"))
        .filter(F.col("event_ts").isNotNull())
        .drop("raw_json")
    )

    write_delta(df, silver_path, mode="overwrite", partition_by=["date_partition"])
    logger.info(f"Silver clickstream: {df.count()} rows written")


def clean_orders(spark) -> None:
    bronze_path = f"s3a://{BUCKET}/bronze/orders"
    silver_path = f"s3a://{BUCKET}/silver/orders"

    df = spark.read.format("delta").load(bronze_path)

    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    df = (
        df.dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("total_amt") > 0)
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("shipped_at", F.to_timestamp("shipped_at"))
        .withColumn("delivered_at", F.to_timestamp("delivered_at"))
        .withColumn("total_amt", F.col("total_amt").cast("double"))
        .withColumn("subtotal", F.col("subtotal").cast("double"))
        .withColumn("tax_amt", F.col("tax_amt").cast("double"))
        .withColumn("shipping_cost", F.col("shipping_cost").cast("double"))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("date_partition", F.to_date("created_at"))
        .drop("raw_json")
    )

    write_delta(df, silver_path, mode="overwrite", partition_by=["date_partition"])
    logger.info(f"Silver orders: {df.count()} rows written")


def clean_users(spark) -> None:
    bronze_path = f"s3a://{BUCKET}/bronze/users"
    silver_path = f"s3a://{BUCKET}/silver/users"

    df = spark.read.format("delta").load(bronze_path)

    df = (
        df.dropDuplicates(["user_id"])
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("email").isNotNull())
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("segment", F.lower(F.trim(F.col("segment"))))
        .withColumn("acquisition_channel", F.lower(F.trim(F.col("acquisition_channel"))))
    )

    upsert_delta(
        spark, df, silver_path,
        merge_condition="target.user_id = source.user_id",
        update_set={c: f"source.{c}" for c in df.columns},
        insert_set={c: f"source.{c}" for c in df.columns},
    )
    logger.info(f"Silver users upserted: {df.count()} rows")


def run_all(spark) -> None:
    clean_clickstream(spark)
    clean_orders(spark)
    clean_users(spark)


if __name__ == "__main__":
    spark = get_spark_session("BronzeToSilver")
    run_all(spark)
    spark.stop()
