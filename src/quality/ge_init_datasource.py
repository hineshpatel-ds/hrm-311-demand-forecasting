import os
import great_expectations as gx
from dotenv import load_dotenv

def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    # Initialize the context - pointing to where your YAML lives
    context = gx.get_context(project_root_dir="./")
    datasource_name = "postgres_hrm311"
    
    # Logic to add if missing
    try:
        context.datasources.get(datasource_name)
        print(f"✅ Datasource '{datasource_name}' already exists.")
    except KeyError:
        context.datasources.add_postgres(
            name=datasource_name, 
            connection_string=db_url
        )
        print(f"🚀 Successfully added modern Fluent Datasource: {datasource_name}")

if __name__ == "__main__":
    main()