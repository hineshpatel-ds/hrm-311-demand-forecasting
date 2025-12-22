import os
import great_expectations as gx
from dotenv import load_dotenv

TABLE = "mart.mart_311_call_volume_30m"
SUITE_NAME = "mart_311_call_volume_30m_suite"

def main():
    load_dotenv()
    context = gx.get_context(project_root_dir="./")
    # 1. Get or create suite
    try:
        suite = context.get_expectation_suite(SUITE_NAME)
        print(f"✅ Found existing suite: {SUITE_NAME}")
    except Exception:
        suite = context.add_expectation_suite(expectation_suite_name=SUITE_NAME)
        print(f"✨ Created new suite: {SUITE_NAME}")

    # 2. Get Datasource
    datasource = context.datasources.get("postgres_hrm311")
    if not datasource:
        raise RuntimeError("Datasource not found. Run ge_init_datasource.py first.")

    # 3. Get or add the Data Asset
    asset_name = "mart_call_volume_asset"
    try:
        asset = datasource.get_asset(asset_name)
    except Exception:
        # We point directly to your table here
        asset = datasource.add_table_asset(name=asset_name, table_name="mart_311_call_volume_30m", schema_name="mart")

    # 4. Get the Validator
    validator = context.get_validator(
        batch_request=asset.build_batch_request(),
        expectation_suite_name=SUITE_NAME,
    )

    # 5. Define Rules
    validator.expect_column_values_to_not_be_null("bucket_ts")
    validator.expect_column_values_to_be_unique("bucket_ts")

    cols_to_check = ["offered", "handled", "abandoned", "processed_in_ivr", "total_talk_time_sec", "avg_talk_time_sec"]
    for col in cols_to_check:
        validator.expect_column_values_to_not_be_null(col)
        validator.expect_column_values_to_be_between(col, min_value=0)

    # 6. Save
    validator.save_expectation_suite()
    print(f"🏁 Suite saved successfully!")

if __name__ == "__main__":
    main()