import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd


def get_clickstream_suite(df: pd.DataFrame) -> dict:
    context = gx.get_context()
    ds = context.sources.add_pandas("bronze_clickstream")
    asset = ds.add_dataframe_asset("clickstream_bronze")
    batch_request = asset.build_batch_request(dataframe=df)

    suite_name = "bronze_clickstream_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    validator.expect_column_to_exist("event_id")
    validator.expect_column_values_to_not_be_null("event_id")
    validator.expect_column_values_to_be_unique("event_id")
    validator.expect_column_to_exist("event_type")
    validator.expect_column_values_to_not_be_null("event_type")
    validator.expect_column_values_to_be_in_set(
        "event_type",
        ["page_view", "click", "add_to_cart", "remove_from_cart", "purchase", "search", "wishlist_add"],
    )
    validator.expect_column_to_exist("timestamp")
    validator.expect_column_values_to_not_be_null("timestamp")
    validator.expect_column_values_to_be_in_set("device", ["desktop", "mobile", "tablet"])

    validator.save_expectation_suite()
    results = validator.validate()
    return results.to_json_dict()


def get_orders_suite(df: pd.DataFrame) -> dict:
    context = gx.get_context()
    ds = context.sources.add_pandas("bronze_orders")
    asset = ds.add_dataframe_asset("orders_bronze")
    batch_request = asset.build_batch_request(dataframe=df)

    suite_name = "bronze_orders_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    validator.expect_column_to_exist("order_id")
    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_be_unique("order_id")
    validator.expect_column_values_to_not_be_null("user_id")
    validator.expect_column_values_to_be_between("total_amt", min_value=0)
    validator.expect_column_values_to_be_in_set(
        "status", ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
    )

    validator.save_expectation_suite()
    results = validator.validate()
    return results.to_json_dict()
