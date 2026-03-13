import os
import json
import logging
from datetime import datetime
from src.processing.utils.spark_session import get_spark_session
from src.quality.expectations.bronze_suite import get_clickstream_suite, get_orders_suite
from src.quality.expectations.silver_suite import get_silver_orders_suite, get_silver_clickstream_suite
from src.quality.expectations.gold_suite import get_daily_revenue_suite

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET", "shopstream")


def validate_layer(layer: str, spark) -> dict[str, dict]:
    results = {}
    timestamp = datetime.utcnow().isoformat()

    if layer in ("bronze", "all"):
        logger.info("Validating bronze clickstream...")
        df = spark.read.format("delta").load(f"s3a://{BUCKET}/bronze/clickstream").limit(100000).toPandas()
        results["bronze_clickstream"] = get_clickstream_suite(df)

        logger.info("Validating bronze orders...")
        df = spark.read.format("delta").load(f"s3a://{BUCKET}/bronze/orders").limit(100000).toPandas()
        results["bronze_orders"] = get_orders_suite(df)

    if layer in ("silver", "all"):
        logger.info("Validating silver clickstream...")
        df = spark.read.format("delta").load(f"s3a://{BUCKET}/silver/clickstream").limit(100000).toPandas()
        results["silver_clickstream"] = get_silver_clickstream_suite(df)

        logger.info("Validating silver orders...")
        df = spark.read.format("delta").load(f"s3a://{BUCKET}/silver/orders").limit(100000).toPandas()
        results["silver_orders"] = get_silver_orders_suite(df)

    if layer in ("gold", "all"):
        logger.info("Validating gold daily revenue...")
        df = spark.read.format("delta").load(f"s3a://{BUCKET}/gold/fct_daily_revenue").toPandas()
        results["gold_daily_revenue"] = get_daily_revenue_suite(df)

    failed = [k for k, v in results.items() if not v.get("success", True)]
    if failed:
        logger.error(f"Validation failed for: {failed}")
        raise ValueError(f"Data quality checks failed: {failed}")

    logger.info(f"All validations passed for layer: {layer}")
    return results


if __name__ == "__main__":
    import sys
    layer = sys.argv[1] if len(sys.argv) > 1 else "all"
    spark = get_spark_session("DataQuality")
    results = validate_layer(layer, spark)
    print(json.dumps({k: v.get("success") for k, v in results.items()}, indent=2))
    spark.stop()
