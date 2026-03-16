import pandas as pd


def transform(df):
    """
    Clean and transform raw parking tickets data.
    Adds derived columns useful for analysis.
    """
    print(f"Transforming {len(df)} records...")

    # ── 1. Drop duplicates ────────────────────────────────────────────────────
    df = df.drop_duplicates(subset="bi_id")
    print(f"  After dedup: {len(df)} records")

    # ── 2. Standardize column types ──────────────────────────────────────────
    df["entrydate"]     = pd.to_datetime(df["entrydate"])
    df["block"]         = pd.to_numeric(df["block"], errors="coerce").astype("Int64")
    df["bylaw"]         = pd.to_numeric(df["bylaw"], errors="coerce").astype("Int64")
    df["bi_id"]         = pd.to_numeric(df["bi_id"], errors="coerce").astype("Int64")
    df["street"]        = df["street"].str.strip().str.upper()
    df["infractiontext"]= df["infractiontext"].str.strip().str.upper()
    df["status"]        = df["status"].str.strip().str.upper()
    df["section"]       = df["section"].str.strip().str.upper()

    # ── 3. Derived columns ───────────────────────────────────────────────────
    df["day_of_week"]   = df["entrydate"].dt.day_name()         # Monday, Tuesday...
    df["month"]         = df["entrydate"].dt.month              # 1-12
    df["month_name"]    = df["entrydate"].dt.strftime("%B")     # January...
    df["is_weekend"]    = df["entrydate"].dt.dayofweek >= 5     # True/False

    # ── 4. Infraction category ───────────────────────────────────────────────
    def categorize(text):
        text = str(text).upper()
        if "METER" in text or "PAY" in text:
            return "Parking Meter"
        elif "PERMIT" in text:
            return "Permit Violation"
        elif "FIRE" in text or "HYDRANT" in text:
            return "Fire Hydrant"
        elif "STOP" in text or "TRAFFIC" in text:
            return "Traffic Sign"
        elif "TIME" in text or "HOUR" in text:
            return "Time Limit"
        elif "DISABLED" in text or "HANDICAP" in text:
            return "Accessible Parking"
        else:
            return "Other"

    df["infraction_category"] = df["infractiontext"].apply(categorize)

    # ── 5. Full address ──────────────────────────────────────────────────────
    df["full_address"] = df["block"].astype(str) + " block " + df["street"]

    # ── 6. Drop raw year column (redundant with entrydate) ───────────────────
    df = df.drop(columns=["year"])

    # ── 7. Rename for clarity ────────────────────────────────────────────────
    df = df.rename(columns={
        "bi_id":         "ticket_id",
        "infractiontext":"infraction_description",
        "entrydate":     "ticket_date"
    })

    # ── 8. Final column order ────────────────────────────────────────────────
    df = df[[
        "ticket_id", "ticket_date", "day_of_week", "month",
        "month_name", "is_weekend", "block", "street", "full_address",
        "bylaw", "section", "status", "infraction_description",
        "infraction_category"
    ]]

    print(f"Transformation complete. Columns: {df.columns.tolist()}")
    return df


if __name__ == "__main__":
    from extract import extract
    raw_df  = extract()
    clean_df = transform(raw_df)
    print(clean_df.head())
    print(f"\nInfraction categories:\n{clean_df['infraction_category'].value_counts()}")
    print(f"\nTop streets:\n{clean_df['street'].value_counts().head(10)}")