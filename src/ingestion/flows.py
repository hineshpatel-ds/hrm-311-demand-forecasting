from prefect import flow, task
from src.ingestion.hrm_311_call_volumes import download_csv, load_to_postgres

@task(retries=2, retry_delay_seconds=30)
def t_download():
    return download_csv()

@task
def t_load(path):
    return load_to_postgres(path)

@flow(name="hrm-311-ingestion")
def hrm_311_ingestion_flow():
    p = t_download()
    rows = t_load(p)
    return rows

if __name__ == "__main__":
    print(f"✅ Rows loaded: {hrm_311_ingestion_flow()}")
