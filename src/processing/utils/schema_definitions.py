from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, TimestampType, ArrayType, MapType, LongType
)

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("anonymous_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("os", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("properties", MapType(StringType(), StringType()), True),
])

ORDER_ITEM_SCHEMA = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_amt", DoubleType(), True),
    StructField("line_total", DoubleType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("items", ArrayType(ORDER_ITEM_SCHEMA), True),
    StructField("item_count", IntegerType(), True),
    StructField("subtotal", DoubleType(), True),
    StructField("shipping_cost", DoubleType(), True),
    StructField("tax_amt", DoubleType(), True),
    StructField("total_amt", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("carrier", StringType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("shipped_at", StringType(), True),
    StructField("delivered_at", StringType(), True),
])

USER_SCHEMA = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("email", StringType(), True),
    StructField("username", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("segment", StringType(), True),
    StructField("acquisition_channel", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])
