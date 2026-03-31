import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_bronze_to_silver")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def make_clickstream_df(spark, records):
    return spark.createDataFrame(records)


class TestCleanClickstream:
   def test_deduplication_removes_duplicates(self, spark):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("page_type", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("device", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("os", StringType(), True),
        StructField("url", StringType(), True),
        StructField("referrer", StringType(), True),  # ← now PySpark knows it's text
        StructField("ip_address", StringType(), True),
        StructField("anonymous_id", StringType(), True),
    ])

    data = [
        ("e1", "s1", "page_view", "2024-01-01T10:00:00", "home", 1, "desktop", "Chrome", "Windows", "http://x.com", None, "1.1.1.1", "a1"),
        ("e1", "s1", "page_view", "2024-01-01T10:00:00", "home", 1, "desktop", "Chrome", "Windows", "http://x.com", None, "1.1.1.1", "a1"),
        ("e2", "s1", "click", "2024-01-01T10:01:00", "product", 1, "desktop", "Chrome", "Windows", "http://x.com/p", None, "1.1.1.1", "a1"),
    ]

    df = spark.createDataFrame(data, schema)
    deduped = df.dropDuplicates(["event_id"])
    assert deduped.count() == 2

    def test_null_event_id_filtered(self, spark):
        data = [
            {"event_id": None, "session_id": "s1"},
            {"event_id": "e1", "session_id": "s1"},
        ]
        df = spark.createDataFrame(data)
        filtered = df.filter(F.col("event_id").isNotNull())
        assert filtered.count() == 1

    def test_device_lowercased(self, spark):
        data = [{"device": "Desktop"}, {"device": "MOBILE"}, {"device": "tablet"}]
        df = spark.createDataFrame(data)
        result = df.withColumn("device", F.lower(F.trim(F.col("device"))))
        devices = [r.device for r in result.collect()]
        assert all(d == d.lower() for d in devices)


class TestCleanOrders:
    def test_negative_total_filtered(self, spark):
        data = [
            {"order_id": "o1", "user_id": 1, "total_amt": 99.99, "status": "confirmed", "created_at": "2024-01-01T10:00:00", "updated_at": "2024-01-01T10:00:00"},
            {"order_id": "o2", "user_id": 2, "total_amt": -5.0, "status": "pending", "created_at": "2024-01-01T10:00:00", "updated_at": "2024-01-01T10:00:00"},
        ]
        df = spark.createDataFrame(data)
        filtered = df.filter(F.col("total_amt") > 0)
        assert filtered.count() == 1
        assert filtered.first().order_id == "o1"

    def test_status_lowercased(self, spark):
        data = [{"status": "CONFIRMED"}, {"status": "Shipped"}, {"status": "delivered"}]
        df = spark.createDataFrame(data)
        result = df.withColumn("status", F.lower(F.trim(F.col("status"))))
        statuses = [r.status for r in result.collect()]
        assert all(s == s.lower() for s in statuses)
