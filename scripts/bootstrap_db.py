from src.utils.db import ensure_schemas

if __name__ == "__main__":
    ensure_schemas()
    print("Schema Ensured: raw, staging and mart")