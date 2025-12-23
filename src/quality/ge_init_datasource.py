import os
import great_expectations as gx
from dotenv import load_dotenv

def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    # Initialize the context from the current directory
    context = gx.get_context(project_root_dir="./")

    datasource_name = "postgres_hrm311"
    
    # Check for both possible names to be safe
    ds_handler = getattr(context, "data_sources", getattr(context, "datasources", None))
    
    if ds_handler is None:
        raise RuntimeError("Could not find datasource attribute on GX context.")

    try:
        ds_handler.get(datasource_name)
        print(f"✅ Datasource '{datasource_name}' already exists.")
    except (KeyError, ValueError):
        # Adding the datasource
        ds_handler.add_postgres(
            name=datasource_name, 
            connection_string=db_url
        )
        print(f"🚀 Successfully added modern Fluent Datasource: {datasource_name}")

if __name__ == "__main__":
    main()