#!/usr/bin/env python3

import os
import sys
import time
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

###############################################################################
# 1) Dictionary of file suffix -> table name
###############################################################################
CSV_SUFFIX_TO_TABLE = {
    "event.checkpoint-results.csv": "json_checkpoint_results",
    "event.details.compounds.csv": "log_compounds",
    "event.details.drivers.csv": "log_drivers",
    "event.details.main.csv": "log_main",
    "event.drivers.csv": "json_drivers",
    "event.event.csv": "json_event",
    "event.fastest-lap-results.csv": "json_fastest_lap_results",
    "event.lap-results.csv": "json_lap_results",
    "event.race-results.csv": "json_race_results",
    # Add more suffixes if needed
}

###############################################################################
# 2) Lock file name; we'll place it in the same directory as this script
###############################################################################
LOCKFILE = os.path.join(os.path.dirname(__file__), "RUNNING")


###############################################################################
# 3) Acquire / release lock functions
###############################################################################
def acquire_lock():
    """
    Wait until RUNNING file does not exist, then create it.
    This ensures only one run at a time.
    """
    i = 0
    while os.path.exists(LOCKFILE):
        if i > 100:
            raise Exception("Waited 100 times, exiting")
        print("Another run is in progress. Waiting...")
        time.sleep(3)
        i += 1
    print("Acquiring lock by creating RUNNING file...")
    with open(LOCKFILE, "w") as f:
        pass  # create empty file


def release_lock():
    """
    Remove the RUNNING file so another run can proceed.
    """
    if os.path.exists(LOCKFILE):
        os.remove(LOCKFILE)
        print("Lock released. RUNNING file removed.")
    else:
        print("Warning: LOCKFILE not found at release time.")


###############################################################################
# 4) Main function
###############################################################################
def import_csv_files(folder_path, db_url, server):
    """
    - Acquire the global lock.
    - Truncate all known tables in the 'source' schema.
    - Scan for CSV files in the given folder, match suffix to table name.
    - Append rows to the truncated tables (if_exists='append').
    - Release the lock at the end.
    """
    acquire_lock()

    # Create DB engine
    engine = create_engine(db_url)

    # Step A: Truncate all known tables
    print("Truncating all known tables in 'source' schema...")
    with engine.begin() as conn:
        for table_name in CSV_SUFFIX_TO_TABLE.values():
            try:
                conn.execute(text(f"TRUNCATE TABLE source.{table_name};"))
                print(f"  -> TRUNCATE source.{table_name}")
            except Exception as e:
                print(
                    f"  -> Could not truncate source.{table_name} (maybe doesn't exist yet). Error: {e}"
                )

    # Step B: Process CSV files
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    csv_files.sort()

    for csv_file in csv_files:
        file_path = os.path.join(folder_path, csv_file)

        matched_table = None
        for suffix, table_name in CSV_SUFFIX_TO_TABLE.items():
            if csv_file.endswith(suffix):
                matched_table = table_name
                break

        if not matched_table:
            print(f"Skipping file (no matching suffix): {csv_file}")
            continue

        print(f"Processing file: {csv_file}")
        print(f" -> Appending to source.{matched_table}")

        try:
            df = pd.read_csv(file_path)

            if matched_table == "json_event":
                df["server"] = server

            df.to_sql(
                matched_table,
                engine,
                schema="source",
                if_exists="append",  # note: appending to the truncated table
                index=False,
            )
        except Exception as e:
            print(f"Error while processing {csv_file}: {e}")

    engine.dispose()
    print("All CSV files processed.")
    release_lock()


###############################################################################
# 5) Script entry point
###############################################################################
if __name__ == "__main__":
    load_dotenv()
    db_url = os.getenv("PG_DATABASE_URL")
    if not db_url:
        print("Error: PG_DATABASE_URL not found in .env")
        sys.exit(1)

    if len(sys.argv) < 3:
        print(
            "Usage: uv run python copy_csv_files_into_db.py.py <folder_path> <event/heat/hotlapping>"
        )
        sys.exit(1)

    folder = sys.argv[1]
    if not os.path.isdir(folder):
        print(f"Error: '{folder}' is not a valid directory.")
        sys.exit(1)

    server = sys.argv[2]
    assert server in [
        "events",
        "heats",
        "hotlapping",
    ], "I only know events/heats/hotlapping as server (second command line argument)"

    import_csv_files(folder, db_url, server)
