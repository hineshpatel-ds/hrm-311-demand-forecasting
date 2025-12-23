import os
import great_expectations as gx
from dotenv import load_dotenv

SUITE_NAME = "mart_311_call_volume_30m_suite"

def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    context = gx.get_context(project_root_dir="./")

    # 1. Use the direct context method your terminal suggested
    try:
        suite = context.add_expectation_suite(expectation_suite_name=SUITE_NAME)
    except Exception:
        # If it exists, just get it
        suite = context.get_expectation_suite(expectation_suite_name=SUITE_NAME)
    
    print(f"✅ Suite ready: {SUITE_NAME}")

    # 2. Get Datasource (Legacy/Early-V1 naming)
    ds_name = "postgres_hrm311"
    try:
        datasource = context.get_datasource(ds_name)
    except Exception:
        # Re-add if missing after your 'rm -rf gx'
        datasource = context.sources.add_postgres(name=ds_name, connection_string=db_url)

    # 3. Get/Create Asset
    asset_name = "mart_call_volume_asset"
    try:
        asset = datasource.get_asset(asset_name)
    except Exception:
        asset = datasource.add_table_asset(
            name=asset_name, 
            table_name="mart_311_call_volume_30m", 
            schema_name="mart"
        )

    # 4. Create Validator
    validator = context.get_validator(
        batch_request=asset.build_batch_request(),
        expectation_suite_name=SUITE_NAME,
    )

    # 5. Define Data Quality Rules
    validator.expect_column_values_to_not_be_null("bucket_ts")
    validator.expect_column_values_to_be_unique("bucket_ts")

    cols = ["offered", "handled", "abandoned", "processed_in_ivr", "total_talk_time_sec", "avg_talk_time_sec"]
    for col in cols:
        validator.expect_column_values_to_not_be_null(col)
        validator.expect_column_values_to_be_between(col, min_value=0)

    # 6. Save directly via Validator (Most stable method)
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"🏁 Suite '{SUITE_NAME}' saved successfully!")

if __name__ == "__main__":
    main()