import os
import logging
from pyspark.sql import DataFrame, functions as F
from src.processing.utils.spark_session import get_spark_session
from src.processing.utils.delta_helpers import write_delta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET", "shopstream")


def build_daily_revenue(spark) -> None:
    orders = spark.read.format("delta").load(f"s3a://{BUCKET}/silver/orders")
    gold_path = f"s3a://{BUCKET}/gold/fct_daily_revenue"

    df = (
        orders.filter(F.col("status").isin("confirmed", "shipped", "delivered"))
        .withColumn("order_date", F.to_date("created_at"))
        .groupBy("order_date")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amt").alias("gross_revenue"),
            F.sum("subtotal").alias("subtotal_revenue"),
            F.sum("tax_amt").alias("total_tax"),
            F.sum("shipping_cost").alias("total_shipping"),
            F.avg("total_amt").alias("avg_order_value"),
            F.countDistinct("user_id").alias("unique_customers"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    write_delta(df, gold_path, mode="overwrite", partition_by=["order_date"])
    logger.info(f"Daily revenue built: {df.count()} days")


def build_conversion_funnel(spark) -> None:
    sessions = spark.read.format("delta").load(f"s3a://{BUCKET}/silver/sessions")
    gold_path = f"s3a://{BUCKET}/gold/fct_conversion_funnel"

    df = (
        sessions.groupBy("date_partition")
        .agg(
            F.count("session_id").alias("total_sessions"),
            F.sum(F.when(F.col("add_to_cart_count") > 0, 1).otherwise(0)).alias("sessions_with_cart"),
            F.sum(F.when(F.col("converted"), 1).otherwise(0)).alias("converted_sessions"),
            F.avg("session_duration_min").alias("avg_session_min"),
            F.avg("event_count").alias("avg_events_per_session"),
        )
        .withColumn("cart_rate", F.col("sessions_with_cart") / F.col("total_sessions"))
        .withColumn("conversion_rate", F.col("converted_sessions") / F.col("total_sessions"))
        .withColumn("processed_at", F.current_timestamp())
    )

    write_delta(df, gold_path, mode="overwrite", partition_by=["date_partition"])
    logger.info(f"Conversion funnel built: {df.count()} days")


def build_customer_metrics(spark) -> None:
    orders = spark.read.format("delta").load(f"s3a://{BUCKET}/silver/orders")
    users = spark.read.format("delta").load(f"s3a://{BUCKET}/silver/users")
    gold_path = f"s3a://{BUCKET}/gold/dim_customers"

    order_agg = (
        orders.filter(F.col("status").isin("confirmed", "shipped", "delivered"))
        .groupBy("user_id")
        .agg(
            F.count("order_id").alias("lifetime_orders"),
            F.sum("total_amt").alias("lifetime_value"),
            F.avg("total_amt").alias("avg_order_value"),
            F.min("created_at").alias("first_order_at"),
            F.max("created_at").alias("last_order_at"),
        )
    )

    df = (
        users.join(order_agg, on="user_id", how="left")
        .withColumn("lifetime_orders", F.coalesce(F.col("lifetime_orders"), F.lit(0)))
        .withColumn("lifetime_value", F.coalesce(F.col("lifetime_value"), F.lit(0.0)))
        .withColumn(
            "customer_tier",
            F.when(F.col("lifetime_value") >= 1000, "platinum")
            .when(F.col("lifetime_value") >= 500, "gold")
            .when(F.col("lifetime_value") >= 100, "silver")
            .otherwise("bronze"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    write_delta(df, gold_path, mode="overwrite")
    logger.info(f"Customer metrics built: {df.count()} customers")


def run_all(spark) -> None:
    build_daily_revenue(spark)
    build_conversion_funnel(spark)
    build_customer_metrics(spark)


if __name__ == "__main__":
    spark = get_spark_session("SilverToGold")
    run_all(spark)
    spark.stop()
