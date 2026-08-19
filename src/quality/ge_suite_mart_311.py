import os

import great_expectations as gx
from dotenv import load_dotenv

SUITE_NAME = "mart_311_call_volume_30m_suite"

def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    context = gx.get_context(project_root_dir="./")

    # 1. Create the suite, or confirm it already exists
    try:
        context.add_expectation_suite(expectation_suite_name=SUITE_NAME)
    except Exception:
        context.get_expectation_suite(expectation_suite_name=SUITE_NAME)

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

    # Upper bounds give headroom over the real observed max (~1,200 for
    # offered) but still catch the class of corrupt source row we found:
    # two 2023/2024 New Year's Day records with OFFERED in the hundreds
    # of thousands.
    max_values = {
        "offered": 5000,
        "handled": 1000,
        "abandoned": 1000,
        "processed_in_ivr": 5000,
        "total_talk_time_sec": 200000,
        "avg_talk_time_sec": 20000,
    }
    for col, max_value in max_values.items():
        validator.expect_column_values_to_not_be_null(col)
        validator.expect_column_values_to_be_between(col, min_value=0, max_value=max_value)

    # 6. Save directly via Validator (Most stable method)
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"🏁 Suite '{SUITE_NAME}' saved successfully!")

if __name__ == "__main__":
    main()