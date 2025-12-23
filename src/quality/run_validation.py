import os
import great_expectations as gx
from dotenv import load_dotenv

SUITE_NAME = "mart_311_call_volume_30m_suite"

def main():
    load_dotenv()
    # Initialize context
    context = gx.get_context(project_root_dir="./")

    # 1. Get the Data Asset
    datasource = context.get_datasource("postgres_hrm311")
    asset = datasource.get_asset("mart_call_volume_asset")
    
    # 2. Create a Validator directly with your saved suite
    # This avoids searching for any Checkpoint .yml files
    print(f"🚀 Validating data against {SUITE_NAME}...")
    validator = context.get_validator(
        batch_request=asset.build_batch_request(),
        expectation_suite_name=SUITE_NAME,
    )

    # 3. Run the validation
    # 'head_only=False' ensures we check the full table in your database
    results = validator.validate()

    # 4. Handle results
    if not results.success:
        print("❌ DATA QUALITY FAILED.")
        context.build_data_docs()
        print("See detailed errors in Data Docs.")
        exit(1)
    
    print("✅ DATA QUALITY PASSED! Your data is ready.")
    context.build_data_docs()

if __name__ == "__main__":
    main()