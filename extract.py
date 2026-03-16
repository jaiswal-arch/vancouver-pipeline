import requests
import pandas as pd

API_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/parking-tickets/records"
MAX_RECORDS = 5000  # Pull 5000 most recent records per run

def extract():
    """
    Extract the most recent parking tickets from Vancouver Open Data API.
    Pulls up to MAX_RECORDS records ordered by most recent first.
    """
    params = {
        "limit": 100,
        "offset": 0,
        "order_by": "entrydate DESC"
    }

    all_records = []
    print(f"Extracting latest {MAX_RECORDS} parking tickets...")

    while len(all_records) < MAX_RECORDS:
        response = requests.get(API_URL, params=params)

        if response.status_code != 200:
            print(f"API error at offset {params['offset']}: {response.status_code}")
            break

        data = response.json()
        records = data.get("results", [])

        if not records:
            break

        all_records.extend(records)
        print(f"  Fetched {len(all_records)} records...")
        params["offset"] += params["limit"]

    df = pd.DataFrame(all_records[:MAX_RECORDS])
    print(f"Extraction complete. Total records: {len(df)}")
    return df


if __name__ == "__main__":
    df = extract()
    print(df.head())
    print(f"\nColumns: {df.columns.tolist()}")
    print(f"Date range: {df['entrydate'].min()} to {df['entrydate'].max()}")