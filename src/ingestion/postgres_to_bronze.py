import os
import logging
from pyspark.sql import DataFrame, functions as F
from src.processing.utils.spark_session import get_spark_session
from src.processing.utils.delta_helpers import write_delta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "shopstream")
PG_USER = os.getenv("POSTGRES_USER", "shopstream")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "shopstream123")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "shopstream")


def read_postgres_table(spark, table: str, where: str | None = None) -> DataFrame:
    query = f"(SELECT * FROM {table}{' WHERE ' + where if where else ''}) AS t"
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", query)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


def extract_table(spark, table: str, partition_col: str | None = None) -> None:
    logger.info(f"Extracting {table} from PostgreSQL")
    df = read_postgres_table(spark, table)
    df = df.withColumn("ingested_at", F.current_timestamp())

    path = f"s3a://{MINIO_BUCKET}/bronze/{table}"
    partition_by = [partition_col] if partition_col else None
    write_delta(df, path, mode="overwrite", partition_by=partition_by)
    logger.info(f"Wrote {df.count()} rows to {path}")


def run_extraction() -> None:
    spark = get_spark_session("PostgresBronze")
    tables = [
        ("users", None),
        ("products", None),
        ("categories", None),
    ]
    for table, partition_col in tables:
        extract_table(spark, table, partition_col)
    spark.stop()


if __name__ == "__main__":
    run_extraction()
