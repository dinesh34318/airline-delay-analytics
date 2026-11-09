# ==============================================================================
# scripts/validate_manager.py
# Stage 5: Comprehensive Data Quality Auditor & Anomaly Detector
# ==============================================================================

import os, sys, csv
import pandas as pd
from datetime import datetime

CSV_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\dataset\raw\monthly_csv"
METADATA_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\dataset\metadata"
LOG_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\logs"
LOG_FILE = os.path.join(LOG_DIR, "inspection.log")
QUALITY_FILE = os.path.join(METADATA_DIR, "data_quality_report.csv")

COLUMN_MAPPINGS = {
    "flight_date": ["FlightDate", "FL_DATE"],
    "airline":     ["Reporting_Airline", "OP_UNIQUE_CARRIER"],
    "flight_num":  ["Flight_Number_Reporting_Airline", "OP_CARRIER_FL_NUM"],
    "origin":      ["Origin"],
    "dest":        ["Dest"],
    "crs_dep":     ["CRSDepTime", "CRS_DEP_TIME"],
    "dep_delay":   ["DepDelay", "DEP_DELAY"],
    "arr_delay":   ["ArrDelay", "ARR_DELAY"],
    "cancelled":   ["Cancelled"],
    "diverted":    ["Diverted"]
}

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line); sys.stdout.flush()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def resolve_cols(header):
    resolved = {}
    for std_name, candidates in COLUMN_MAPPINGS.items():
        matched = [c for c in candidates if c in header]
        resolved[std_name] = matched[0] if matched else None
    return resolved

def audit_month(filepath, month):
    fname = os.path.basename(filepath)
    log(f"Month {month:02d}: Running quality audit on {fname}...")
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        header = next(csv.reader(f))
    col_map = resolve_cols(header)
    cols_to_read = [v for v in col_map.values() if v is not None]
    inv_map = {v: k for k, v in col_map.items() if v is not None}

    n_total = 0
    n_missing_key = 0
    n_invalid_time = 0
    n_time_2400 = 0
    n_arr_extreme = 0
    n_dep_extreme = 0
    n_cancelled = 0
    n_diverted = 0
    n_valid_arr = 0
    n_valid_dep = 0
    n_canc_with_arr = 0

    for chunk in pd.read_csv(filepath, usecols=cols_to_read, chunksize=200000,
                              low_memory=False, encoding="utf-8", on_bad_lines="skip"):
        chunk.rename(columns=inv_map, inplace=True)
        n_total += len(chunk)

        for col in ["flight_date","airline","flight_num","origin","dest","crs_dep","cancelled"]:
            if col in chunk.columns:
                n_missing_key += chunk[col].isna().sum()

        if "crs_dep" in chunk.columns:
            cd = pd.to_numeric(chunk["crs_dep"], errors="coerce")
            n_time_2400 += int((cd == 2400).sum())
            hr = cd // 100; mn = cd % 100
            invalid = cd.isna() | (cd < 0) | ((cd != 2400) & ((hr > 23) | (mn > 59)))
            n_invalid_time += int(invalid.sum())

        if "arr_delay" in chunk.columns:
            ad = pd.to_numeric(chunk["arr_delay"], errors="coerce")
            n_arr_extreme += int(((ad < -120) | (ad > 3000)).sum())
        if "dep_delay" in chunk.columns:
            dd = pd.to_numeric(chunk["dep_delay"], errors="coerce")
            n_dep_extreme += int(((dd < -120) | (dd > 3000)).sum())

        if "cancelled" in chunk.columns:
            c = pd.to_numeric(chunk["cancelled"], errors="coerce")
            n_cancelled += int((c == 1).sum())
            if "arr_delay" in chunk.columns:
                ad = pd.to_numeric(chunk["arr_delay"], errors="coerce")
                n_canc_with_arr += int(((c == 1) & ad.notna()).sum())
        if "diverted" in chunk.columns:
            d = pd.to_numeric(chunk["diverted"], errors="coerce")
            n_diverted += int((d == 1).sum())

        if all(c in chunk.columns for c in ["cancelled","diverted","arr_delay"]):
            c = pd.to_numeric(chunk["cancelled"], errors="coerce").fillna(1)
            dv = pd.to_numeric(chunk["diverted"], errors="coerce").fillna(1)
            ad = pd.to_numeric(chunk["arr_delay"], errors="coerce")
            n_valid_arr += int(((c == 0) & (dv == 0) & ad.notna()).sum())
        if all(c in chunk.columns for c in ["cancelled","dep_delay"]):
            c = pd.to_numeric(chunk["cancelled"], errors="coerce").fillna(1)
            dd = pd.to_numeric(chunk["dep_delay"], errors="coerce")
            n_valid_dep += int(((c == 0) & dd.notna()).sum())

    log(f"Month {month:02d}: {n_total:,} rows | Cancelled={n_cancelled:,} | Valid Arr={n_valid_arr:,} | Invalid Time={n_invalid_time:,} | 2400 Rollovers={n_time_2400:,}")

    return {
        "month": month, "filename": fname, "total_rows": n_total,
        "missing_essential_key_values": n_missing_key,
        "invalid_scheduled_time_count": n_invalid_time,
        "time_2400_rollover_count": n_time_2400,
        "cancelled_count": n_cancelled, "diverted_count": n_diverted,
        "valid_arrival_records": n_valid_arr, "valid_departure_records": n_valid_dep,
        "arr_delay_extreme_values": n_arr_extreme, "dep_delay_extreme_values": n_dep_extreme,
        "cancelled_flights_with_arr_delay": n_canc_with_arr
    }

def main():
    log("=" * 70)
    log("Starting Stage 5: Data Quality Validation (All 12 Months)")
    log("=" * 70)
    results = []
    for m in range(1, 13):
        fp = os.path.join(CSV_DIR, f"On_Time_Reporting_Carrier_On_Time_Performance_2025_{m}.csv")
        if not os.path.exists(fp):
            log(f"MISSING: Month {m}"); continue
        results.append(audit_month(fp, m))

    df = pd.DataFrame(results)
    df.to_csv(QUALITY_FILE, index=False)
    log(f"Quality report saved: {QUALITY_FILE}")
    log("=" * 70)
    log(f"STAGE 5 COMPLETE. Total Rows Audited: {df['total_rows'].sum():,}")
    log(f"Total Cancelled: {df['cancelled_count'].sum():,} | Total 2400 Rollovers: {df['time_2400_rollover_count'].sum():,}")
    log(f"Total Valid Arrivals: {df['valid_arrival_records'].sum():,} | Valid Departures: {df['valid_departure_records'].sum():,}")
    log("=" * 70)

if __name__ == "__main__":
    main()
