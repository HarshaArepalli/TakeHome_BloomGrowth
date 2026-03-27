#!/usr/bin/env python3
"""
Load CSV seed files into DuckDB for analysis.
This is a workaround for Python 3.14 compatibility issues with dbt.
"""
import duckdb
import pandas as pd
import os
from pathlib import Path

# Connect to DuckDB
db_path = "/Users/shivachaithanyagoli/Desktop/harsha_interview_assessment/analytics.duckdb"
conn = duckdb.connect(db_path)

# Define seed files and their target table names
seeds_dir = "/Users/shivachaithanyagoli/Desktop/harsha_interview_assessment/seeds"
seed_files = {
    "organizations.csv": "organizations",
    "plans.csv": "plans",
    "subscriptions.csv": "subscriptions",
    "users.csv": "users",
    "activity_events.csv": "activity_events"
}

print("Loading seed files into DuckDB...")
for csv_file, table_name in seed_files.items():
    csv_path = os.path.join(seeds_dir, csv_file)
    if os.path.exists(csv_path):
        print(f"  Loading {csv_file} into {table_name}...")
        try:
            # Drop table if exists
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            # Load CSV directly using DuckDB
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')")
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
            print(f"    ✓ Loaded {count} rows")
        except Exception as e:
            print(f"    ✗ Error loading {csv_file}: {e}")
    else:
        print(f"  ✗ {csv_file} not found!")

print("\nAll seed files loaded successfully!")
conn.close()
