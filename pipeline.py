from extract import extract
from transform import transform
from load import load
import traceback
from datetime import datetime

def run_pipeline():
    start = datetime.now()
    print(f"Pipeline started at {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    try:
        # Step 1 — Extract
        print("\n[1/3] EXTRACT")
        raw_df = extract()

        # Step 2 — Transform
        print("\n[2/3] TRANSFORM")
        clean_df = transform(raw_df)

        # Step 3 — Load
        print("\n[3/3] LOAD")
        load(clean_df)

        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)
        print("\n" + "=" * 50)
        print(f"Pipeline completed successfully in {duration}s")

    except Exception as e:
        print("\n" + "=" * 50)
        print(f"Pipeline FAILED: {e}")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_pipeline()