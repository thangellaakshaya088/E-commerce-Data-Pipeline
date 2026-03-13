import great_expectations as gx
import pandas as pd


def get_silver_orders_suite(df: pd.DataFrame) -> dict:
    context = gx.get_context()
    ds = context.sources.add_pandas("silver_orders")
    asset = ds.add_dataframe_asset("orders_silver")
    batch_request = asset.build_batch_request(dataframe=df)

    suite_name = "silver_orders_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_be_unique("order_id")
    validator.expect_column_values_to_not_be_null("user_id")
    validator.expect_column_values_to_not_be_null("created_at")
    validator.expect_column_values_to_be_between("total_amt", min_value=0.01)
    validator.expect_column_values_to_be_between("item_count", min_value=1, max_value=100)
    validator.expect_column_values_to_not_be_null("status")

    validator.save_expectation_suite()
    return validator.validate().to_json_dict()


def get_silver_clickstream_suite(df: pd.DataFrame) -> dict:
    context = gx.get_context()
    ds = context.sources.add_pandas("silver_clickstream")
    asset = ds.add_dataframe_asset("clickstream_silver")
    batch_request = asset.build_batch_request(dataframe=df)

    suite_name = "silver_clickstream_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    validator.expect_column_values_to_not_be_null("event_id")
    validator.expect_column_values_to_be_unique("event_id")
    validator.expect_column_values_to_not_be_null("event_ts")
    validator.expect_column_values_to_not_be_null("session_id")

    validator.save_expectation_suite()
    return validator.validate().to_json_dict()
