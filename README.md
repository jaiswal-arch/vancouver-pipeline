# 🚗 Vancouver Parking Tickets Pipeline

> Automated end-to-end data pipeline that extracts Vancouver parking ticket data from the city's open data API, transforms it with Python, and loads it into Google BigQuery — scheduled daily via GitHub Actions.

---

## Architecture

```
Vancouver Open Data API
        ↓
   extract.py (Pull 5,000 latest records)
        ↓
   transform.py (Clean, standardize, enrich)
        ↓
   load.py (Push to Google BigQuery)
        ↓
GitHub Actions (Scheduled daily at 8AM UTC)
```

---

## What It Does

**Extract:** Pulls the 5,000 most recent parking tickets from Vancouver's Open Data API — 2.4M+ records in the source dataset covering tickets issued across the city.

**Transform:** Cleans and enriches the raw data by standardizing text fields, converting data types, adding derived columns (day of week, month, weekend flag), categorizing infractions into readable groups, and combining block + street into a full address.

**Load:** Pushes the cleaned dataset into a BigQuery table, replacing it on every run to ensure the table always reflects the latest available data.

**Schedule:** GitHub Actions runs the full pipeline automatically every day at 8AM UTC. Can also be triggered manually from the Actions tab.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Extraction | Python `requests` + Vancouver Open Data API |
| Transformation | Python `pandas` |
| Loading | Google BigQuery (`google-cloud-bigquery`) |
| Orchestration | GitHub Actions |
| Cloud Warehouse | Google BigQuery |

---

## Dataset

**Source:** [City of Vancouver Open Data Portal — Parking Tickets](https://opendata.vancouver.ca/explore/dataset/parking-tickets)

| Field | Description |
|---|---|
| `ticket_id` | Unique ticket identifier |
| `ticket_date` | Date ticket was issued |
| `day_of_week` | Day name (Monday–Sunday) |
| `month` / `month_name` | Month number and name |
| `is_weekend` | True if Saturday or Sunday |
| `block` / `street` | Location of infraction |
| `full_address` | Combined block + street |
| `bylaw` / `section` | Bylaw reference |
| `status` | Ticket status |
| `infraction_description` | Full legal infraction text |
| `infraction_category` | Simplified category (Parking Meter, Traffic Sign, etc.) |

---

## Project Structure

```
vancouver-parking-pipeline/
├── extract.py                  # API extraction logic
├── transform.py                # Data cleaning and enrichment
├── load.py                     # BigQuery loading logic
├── pipeline.py                 # Orchestrates all three steps
├── requirements.txt            # Python dependencies
├── README.md
└── .github/
    └── workflows/
        └── pipeline.yml        # GitHub Actions schedule
```

---

## How to Run Locally

```bash
# Clone the repo
git clone https://github.com/jaiswal-arch/vancouver-parking-pipeline

# Install dependencies
pip install -r requirements.txt

# Add your GCP service account key
# Place service_account.json in the root folder

# Run the full pipeline
python pipeline.py
```

---

## Sample Output

```
Pipeline started at 2026-03-15 21:23:43
==================================================
[1/3] EXTRACT
Extracting latest 5000 parking tickets...
Extraction complete. Total records: 5000

[2/3] TRANSFORM
Transforming 5000 records...
After dedup: 5000 records
Transformation complete.

[3/3] LOAD
Loading 5000 records into e-commerece-489804.vancouver_pipeline.parking_tickets...
Load complete. 5000 rows now in BigQuery

==================================================
Pipeline completed successfully in 32.8s
```

---

## Key Insights from the Data

- **Traffic Sign violations** are the most common infraction - 2,729 out of 5,000 tickets
- **Richards St** has the highest ticket concentration in the dataset
- **91%+ of tickets** are issued on weekdays - parking enforcement is weekday-heavy
- **December** is the most represented month in the current dataset

---

## Notes

- The Vancouver Open Data API has a maximum offset of 10,000 records - the pipeline is capped at 5,000 per run to stay within limits
- The source dataset is updated periodically by the City of Vancouver - not in real time
- `service_account.json` is excluded from the repo via `.gitignore` - credentials are stored as a GitHub Secret (`GCP_SERVICE_ACCOUNT_KEY`)