import os
import logging
from pathlib import Path
from pyspark.sql import DataFrame, functions as F
from src.processing.utils.spark_session import get_spark_session
from src.processing.utils.delta_helpers import write_delta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "shopstream")


def load_csv(spark, file_path: str, infer_schema: bool = True) -> DataFrame:
    return spark.read.option("header", "true").option("inferSchema", str(infer_schema)).csv(file_path)


def load_json(spark, file_path: str) -> DataFrame:
    return spark.read.option("multiline", "true").json(file_path)


def load_file_to_bronze(spark, file_path: str, table_name: str) -> None:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"{file_path} not found")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = load_csv(spark, file_path)
    elif suffix == ".json":
        df = load_json(spark, file_path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    df = df.withColumn("source_file", F.lit(path.name)).withColumn("ingested_at", F.current_timestamp())
    bronze_path = f"s3a://{MINIO_BUCKET}/bronze/{table_name}"
    write_delta(df, bronze_path, mode="append")
    logger.info(f"Loaded {df.count()} rows from {file_path} to {bronze_path}")


if __name__ == "__main__":
    import sys
    file_path = sys.argv[1]
    table_name = sys.argv[2]
    spark = get_spark_session("FileToBronze")
    load_file_to_bronze(spark, file_path, table_name)
    spark.stop()
