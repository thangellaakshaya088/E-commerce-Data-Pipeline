import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_silver_to_gold")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


class TestDailyRevenue:
    def test_revenue_aggregation(self, spark):
        data = [
            {"order_id": "o1", "user_id": 1, "status": "delivered", "total_amt": 100.0, "subtotal": 90.0, "tax_amt": 8.0, "shipping_cost": 2.0, "created_at": "2024-01-15T10:00:00"},
            {"order_id": "o2", "user_id": 2, "status": "confirmed", "total_amt": 200.0, "subtotal": 180.0, "tax_amt": 15.0, "shipping_cost": 5.0, "created_at": "2024-01-15T11:00:00"},
            {"order_id": "o3", "user_id": 3, "status": "cancelled", "total_amt": 50.0, "subtotal": 45.0, "tax_amt": 4.0, "shipping_cost": 1.0, "created_at": "2024-01-15T12:00:00"},
        ]
        df = spark.createDataFrame(data)
        df = df.withColumn("created_at", F.to_timestamp("created_at"))

        result = (
            df.filter(F.col("status").isin("confirmed", "shipped", "delivered"))
            .withColumn("order_date", F.to_date("created_at"))
            .groupBy("order_date")
            .agg(
                F.count("order_id").alias("order_count"),
                F.sum("total_amt").alias("gross_revenue"),
            )
        )

        row = result.first()
        assert row.order_count == 2
        assert row.gross_revenue == pytest.approx(300.0)

    def test_unique_customers_counted(self, spark):
        data = [
            {"order_id": "o1", "user_id": 1, "status": "delivered", "total_amt": 100.0, "created_at": "2024-01-15T10:00:00"},
            {"order_id": "o2", "user_id": 1, "status": "delivered", "total_amt": 50.0, "created_at": "2024-01-15T11:00:00"},
            {"order_id": "o3", "user_id": 2, "status": "confirmed", "total_amt": 75.0, "created_at": "2024-01-15T12:00:00"},
        ]
        df = spark.createDataFrame(data)
        df = df.withColumn("created_at", F.to_timestamp("created_at"))

        result = (
            df.filter(F.col("status").isin("confirmed", "shipped", "delivered"))
            .withColumn("order_date", F.to_date("created_at"))
            .groupBy("order_date")
            .agg(F.countDistinct("user_id").alias("unique_customers"))
        )
        assert result.first().unique_customers == 2


class TestCustomerTier:
    def test_customer_tier_logic(self, spark):
        data = [
            {"user_id": 1, "lifetime_value": 1500.0},
            {"user_id": 2, "lifetime_value": 700.0},
            {"user_id": 3, "lifetime_value": 150.0},
            {"user_id": 4, "lifetime_value": 50.0},
        ]
        df = spark.createDataFrame(data)
        result = df.withColumn(
            "tier",
            F.when(F.col("lifetime_value") >= 1000, "platinum")
            .when(F.col("lifetime_value") >= 500, "gold")
            .when(F.col("lifetime_value") >= 100, "silver")
            .otherwise("bronze"),
        )

        tiers = {r.user_id: r.tier for r in result.collect()}
        assert tiers[1] == "platinum"
        assert tiers[2] == "gold"
        assert tiers[3] == "silver"
        assert tiers[4] == "bronze"
