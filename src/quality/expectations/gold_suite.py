import great_expectations as gx
import pandas as pd


def get_daily_revenue_suite(df: pd.DataFrame) -> dict:
    context = gx.get_context()
    ds = context.sources.add_pandas("gold_revenue")
    asset = ds.add_dataframe_asset("daily_revenue_gold")
    batch_request = asset.build_batch_request(dataframe=df)

    suite_name = "gold_daily_revenue_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    validator.expect_column_values_to_not_be_null("order_date")
    validator.expect_column_values_to_be_unique("order_date")
    validator.expect_column_values_to_be_between("gross_revenue", min_value=0)
    validator.expect_column_values_to_be_between("order_count", min_value=0)
    validator.expect_column_values_to_be_between("avg_order_value", min_value=0)
    validator.expect_column_values_to_be_between("unique_customers", min_value=0)

    validator.save_expectation_suite()
    return validator.validate().to_json_dict()
