# ==============================================================================
# scripts/inspect_manager.py
# Stage 4: Dataset Profiler, Schema Mapping & Data Dictionary Generator
# ==============================================================================

import os
import sys
import csv
import pandas as pd
from datetime import datetime

CSV_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\dataset\raw\monthly_csv"
METADATA_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\dataset\metadata"
LOG_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\logs"

os.makedirs(METADATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

MONTHLY_SUMMARY_FILE = os.path.join(METADATA_DIR, "monthly_summary.csv")
DICT_FILE = os.path.join(METADATA_DIR, "dataset_dictionary.csv")
LOG_FILE = os.path.join(LOG_DIR, "inspection.log")

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)
    sys.stdout.flush()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

COLUMN_MAPPINGS = {
    "flight_date": ["FlightDate", "FL_DATE"],
    "airline":     ["Reporting_Airline", "OP_UNIQUE_CARRIER", "UniqueCarrier", "DOT_ID_Reporting_Airline"],
    "flight_num":  ["Flight_Number_Reporting_Airline", "OP_CARRIER_FL_NUM", "FlightNum"],
    "origin":      ["Origin", "ORIGIN"],
    "dest":        ["Dest", "DEST"],
    "crs_dep":     ["CRSDepTime", "CRS_DEP_TIME"],
    "dep_delay":   ["DepDelay", "DEP_DELAY"],
    "arr_delay":   ["ArrDelay", "ARR_DELAY"],
    "cancelled":   ["Cancelled", "CANCELLED"],
    "diverted":    ["Diverted", "DIVERTED"]
}

def resolve_cols(header):
    resolved = {}
    for std_name, candidates in COLUMN_MAPPINGS.items():
        matched = [c for c in candidates if c in header]
        if matched:
            resolved[std_name] = matched[0]
        else:
            resolved[std_name] = None
    return resolved

def inspect_file(filepath, month):
    fname = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)
    log(f"Month {month:02d}: Inspecting {fname} ({file_size:,} bytes)...")
    
    # Read header
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
    
    total_cols = len(header)
    col_map = resolve_cols(header)
    
    missing_required = [k for k, v in col_map.items() if v is None]
    if missing_required:
        log(f"WARNING: Month {month:02d} missing columns: {missing_required}")
    
    # Selected columns to read
    cols_to_read = [v for v in col_map.values() if v is not None]
    inv_map = {v: k for k, v in col_map.items() if v is not None}
    
    # Read chunks to compute exact row counts and missing values
    chunk_size = 200000
    total_rows = 0
    missing_counts = {k: 0 for k in COLUMN_MAPPINGS.keys()}
    min_date = None
    max_date = None
    cancelled_count = 0
    diverted_count = 0
    
    for chunk in pd.read_csv(filepath, usecols=cols_to_read, chunksize=chunk_size, low_memory=False, encoding="utf-8", on_bad_lines="skip"):
        chunk.rename(columns=inv_map, inplace=True)
        total_rows += len(chunk)
        
        for k in missing_counts.keys():
            if k in chunk.columns:
                missing_counts[k] += chunk[k].isna().sum()
        
        if "flight_date" in chunk.columns:
            cur_min = chunk["flight_date"].dropna().min()
            cur_max = chunk["flight_date"].dropna().max()
            if min_date is None or (cur_min and cur_min < min_date):
                min_date = cur_min
            if max_date is None or (cur_max and cur_max > max_date):
                max_date = cur_max
        
        if "cancelled" in chunk.columns:
            cancelled_count += (chunk["cancelled"] == 1).sum()
        if "diverted" in chunk.columns:
            diverted_count += (chunk["diverted"] == 1).sum()
    
    log(f"Month {month:02d}: Completed {fname} -> {total_rows:,} rows, Dates: {min_date} to {max_date}")
    
    return {
        "month": month,
        "filename": fname,
        "size_bytes": file_size,
        "total_columns": total_cols,
        "total_rows": total_rows,
        "start_date": min_date,
        "end_date": max_date,
        "cancelled_count": cancelled_count,
        "diverted_count": diverted_count,
        "missing_flight_date": missing_counts["flight_date"],
        "missing_airline": missing_counts["airline"],
        "missing_flight_num": missing_counts["flight_num"],
        "missing_origin": missing_counts["origin"],
        "missing_dest": missing_counts["dest"],
        "missing_crs_dep": missing_counts["crs_dep"],
        "missing_dep_delay": missing_counts["dep_delay"],
        "missing_arr_delay": missing_counts["arr_delay"]
    }

def generate_dictionary():
    dict_records = [
        {"standard_column": "flight_date", "data_type": "STRING (YYYY-MM-DD)", "canonical_bts_name": "FlightDate", "role": "Partitioning key, temporal joins"},
        {"standard_column": "airline", "data_type": "STRING (IATA/DOT)", "canonical_bts_name": "Reporting_Airline", "role": "Workload W1 carrier grouping"},
        {"standard_column": "flight_num", "data_type": "INT32", "canonical_bts_name": "Flight_Number_Reporting_Airline", "role": "Structured key uniqueness component"},
        {"standard_column": "origin", "data_type": "STRING (IATA)", "canonical_bts_name": "Origin", "role": "Workload W2 origin grouping, W4 route key"},
        {"standard_column": "dest", "data_type": "STRING (IATA)", "canonical_bts_name": "Dest", "role": "Workload W4 destination grouping"},
        {"standard_column": "crs_dep", "data_type": "INT32 (HHMM)", "canonical_bts_name": "CRSDepTime", "role": "Scheduled departure local time"},
        {"standard_column": "dep_delay", "data_type": "DOUBLE (Minutes)", "canonical_bts_name": "DepDelay", "role": "Workload W2 departure delay measure"},
        {"standard_column": "arr_delay", "data_type": "DOUBLE (Minutes)", "canonical_bts_name": "ArrDelay", "role": "Workloads W1, W3, W4 arrival delay measure"},
        {"standard_column": "cancelled", "data_type": "INT32 (0/1)", "canonical_bts_name": "Cancelled", "role": "Workload W1 flight cancellation metric"},
        {"standard_column": "diverted", "data_type": "INT32 (0/1)", "canonical_bts_name": "Diverted", "role": "Routing validity filtering"},
        {"standard_column": "sched_dep_hour", "data_type": "INT32 (0-23)", "canonical_bts_name": "DERIVED (floor(CRSDepTime/100))", "role": "Workload W2 hourly scheduled departure key"},
        {"standard_column": "sched_dep_local", "data_type": "TIMESTAMP", "canonical_bts_name": "DERIVED (Date + CRSDepTime)", "role": "Workload W3 local diurnal windowing"},
        {"standard_column": "event_id", "data_type": "STRING (SHA-256)", "canonical_bts_name": "DERIVED (SHA-256 Hash)", "role": "Kafka streaming event key & deduplication"},
        {"standard_column": "is_valid_arr", "data_type": "INT32 (0/1)", "canonical_bts_name": "DERIVED (Validity Flag)", "role": "Zero-imputation arrival validity indicator"},
        {"standard_column": "is_valid_dep", "data_type": "INT32 (0/1)", "canonical_bts_name": "DERIVED (Validity Flag)", "role": "Zero-imputation departure validity indicator"},
        {"standard_column": "source_file", "data_type": "STRING", "canonical_bts_name": "METADATA", "role": "Source traceability audit trail"},
        {"standard_column": "source_row_num", "data_type": "INT64", "canonical_bts_name": "METADATA", "role": "Source row number traceability"},
        {"standard_column": "raw_row_hash", "data_type": "STRING (SHA-256)", "canonical_bts_name": "METADATA", "role": "Source row cryptographic integrity hash"}
    ]
    df = pd.DataFrame(dict_records)
    df.to_csv(DICT_FILE, index=False)
    log(f"Dataset dictionary written to: {DICT_FILE}")

def main():
    log("=" * 70)
    log("Starting Stage 4: Dataset Inspection & Profiling (All 12 Months)")
    log("=" * 70)
    
    results = []
    total_records = 0
    
    for m in range(1, 13):
        csv_filename = f"On_Time_Reporting_Carrier_On_Time_Performance_2025_{m}.csv"
        csv_path = os.path.join(CSV_DIR, csv_filename)
        
        if not os.path.exists(csv_path):
            log(f"ERROR: Missing CSV for month {m}: {csv_path}")
            continue
        
        res = inspect_file(csv_path, m)
        results.append(res)
        total_records += res["total_rows"]
    
    summary_df = pd.DataFrame(results)
    summary_df.to_csv(MONTHLY_SUMMARY_FILE, index=False)
    log(f"Monthly summary written to: {MONTHLY_SUMMARY_FILE}")
    
    generate_dictionary()
    
    log("=" * 70)
    log(f"STAGE 4 COMPLETE. Total Annual Records Profiled: {total_records:,}")
    log("=" * 70)

if __name__ == "__main__":
    main()
