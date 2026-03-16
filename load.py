from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os

# ── Configuration ─────────────────────────────────────────────────────────────
PROJECT_ID   = "e-commerece-489804"
DATASET_ID   = "vancouver_pipeline"
TABLE_ID     = "parking_tickets"
KEY_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", 
           r"C:\Users\archi\Documents\gcp_keys\service_account.json")
FULL_TABLE   = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def get_client():
    """Create and return an authenticated BigQuery client."""
    credentials = service_account.Credentials.from_service_account_file(
        KEY_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    return client


def load(df):
    """
    Load transformed DataFrame into BigQuery.
    Uses WRITE_TRUNCATE — replaces the table on every run.
    """
    print(f"Loading {len(df)} records into {FULL_TABLE}...")

    client = get_client()

    # Define schema explicitly so BigQuery gets the types right
    schema = [
        bigquery.SchemaField("ticket_id",              "INTEGER"),
        bigquery.SchemaField("ticket_date",            "DATE"),
        bigquery.SchemaField("day_of_week",            "STRING"),
        bigquery.SchemaField("month",                  "INTEGER"),
        bigquery.SchemaField("month_name",             "STRING"),
        bigquery.SchemaField("is_weekend",             "BOOLEAN"),
        bigquery.SchemaField("block",                  "INTEGER"),
        bigquery.SchemaField("street",                 "STRING"),
        bigquery.SchemaField("full_address",           "STRING"),
        bigquery.SchemaField("bylaw",                  "INTEGER"),
        bigquery.SchemaField("section",                "STRING"),
        bigquery.SchemaField("status",                 "STRING"),
        bigquery.SchemaField("infraction_description", "STRING"),
        bigquery.SchemaField("infraction_category",    "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Convert pandas nullable integers to standard before loading
    for col in ["ticket_id", "block", "bylaw", "month"]:
        df[col] = df[col].astype("float64").astype("Int64")

    # Convert ticket_date to python date object for BigQuery DATE type
    df["ticket_date"] = pd.to_datetime(df["ticket_date"]).dt.date

    job = client.load_table_from_dataframe(df, FULL_TABLE, job_config=job_config)
    job.result()  # Wait for the job to complete

    # Verify
    table = client.get_table(FULL_TABLE)
    print(f"Load complete. {table.num_rows} rows now in {FULL_TABLE}")


if __name__ == "__main__":
    from extract import extract
    from transform import transform

    raw_df   = extract()
    clean_df = transform(raw_df)
    load(clean_df)