# ==============================================================================
# scripts/cleanse_manager.py
# Stage 6: Chunked Cleansing, Quarantine, 2400 Rollover Fix,
#           SHA-256 Event IDs, and Parquet / Clean-CSV Generation
# ==============================================================================
# Technology: Pure Python + pandas (chunked, low-memory)
# Downstream consumers: R (Stage 7 profiling), Hive (Stage 8 analytics),
#                       Java Kafka producer (Stage 9 streaming)
# ==============================================================================

import os, sys, csv, hashlib, json, io
import pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
from datetime import datetime, date, timedelta

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR      = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics"
CSV_DIR       = os.path.join(BASE_DIR, "dataset", "raw",       "monthly_csv")
CLEAN_CSV_DIR = os.path.join(BASE_DIR, "dataset", "processed", "clean_csv")
PARQUET_DIR   = os.path.join(BASE_DIR, "dataset", "processed", "parquet")
QUARANTINE    = os.path.join(BASE_DIR, "dataset", "processed", "quarantine")
METADATA_DIR  = os.path.join(BASE_DIR, "dataset", "metadata")
LOG_DIR       = os.path.join(BASE_DIR, "logs")
LOG_FILE      = os.path.join(LOG_DIR,  "cleanse.log")
CLEANSE_RPT   = os.path.join(METADATA_DIR, "cleanse_report.csv")

for d in [CLEAN_CSV_DIR, PARQUET_DIR, QUARANTINE, METADATA_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

CHUNK_SIZE = 150_000   # rows per pandas chunk — safe on 8 GB RAM

# ── Essential columns (must all be present & non-null) ──────────────────────
ESSENTIAL = {
    "flight_date":  ["FlightDate",  "FL_DATE"],
    "airline":      ["Reporting_Airline", "OP_UNIQUE_CARRIER"],
    "flight_num":   ["Flight_Number_Reporting_Airline", "OP_CARRIER_FL_NUM"],
    "origin":       ["Origin"],
    "dest":         ["Dest"],
    "crs_dep":      ["CRSDepTime",  "CRS_DEP_TIME"],
    "cancelled":    ["Cancelled"],
}

# ── Canonical output column set ───────────────────────────────────────────────
# Keeps only columns relevant to delay analytics; drops BTS fill columns
# that are always empty in 2024-25 schema (dup arrival columns, etc.)
KEEP_STD = [
    "event_id",          # SHA-256 derived surrogate key
    "flight_date", "airline", "flight_num", "tail_num",
    "origin", "dest",
    "crs_dep", "dep_time", "dep_delay", "dep_delay_15",
    "crs_arr", "arr_time", "arr_delay", "arr_delay_15",
    "cancelled", "cancellation_code", "diverted",
    "crs_elapsed", "actual_elapsed", "air_time",
    "distance",
    "carrier_delay", "weather_delay", "nas_delay",
    "security_delay", "late_aircraft_delay",
    "dep_time_fixed",    # 2400→0000 next-day corrected departure time
    "is_2400_rollover",  # flag: original CRSDepTime was 2400
    "quarantine_reason", # blank for clean rows
]

# ── Column alias map (raw BTS name → canonical std name) ─────────────────────
ALIAS = {
    "FlightDate": "flight_date", "FL_DATE": "flight_date",
    "Reporting_Airline": "airline", "OP_UNIQUE_CARRIER": "airline",
    "Flight_Number_Reporting_Airline": "flight_num", "OP_CARRIER_FL_NUM": "flight_num",
    "Tail_Number": "tail_num", "TAIL_NUM": "tail_num",
    "Origin": "origin", "Dest": "dest",
    "CRSDepTime": "crs_dep",   "CRS_DEP_TIME": "crs_dep",
    "DepTime":    "dep_time",  "DEP_TIME":    "dep_time",
    "DepDelay":   "dep_delay", "DEP_DELAY":   "dep_delay",
    "DepDel15":   "dep_delay_15", "DEP_DEL15": "dep_delay_15",
    "CRSArrTime": "crs_arr",   "CRS_ARR_TIME": "crs_arr",
    "ArrTime":    "arr_time",  "ARR_TIME":    "arr_time",
    "ArrDelay":   "arr_delay", "ARR_DELAY":   "arr_delay",
    "ArrDel15":   "arr_delay_15", "ARR_DEL15": "arr_delay_15",
    "Cancelled":  "cancelled", "Diverted": "diverted",
    "CancellationCode": "cancellation_code",
    "CRSElapsedTime": "crs_elapsed", "CRS_ELAPSED_TIME": "crs_elapsed",
    "ActualElapsedTime": "actual_elapsed",
    "AirTime":    "air_time",
    "Distance":   "distance",
    "CarrierDelay":      "carrier_delay",
    "WeatherDelay":      "weather_delay",
    "NASDelay":          "nas_delay",
    "SecurityDelay":     "security_delay",
    "LateAircraftDelay": "late_aircraft_delay",
}

# ── Logging ───────────────────────────────────────────────────────────────────
def log(msg):
    ts   = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line); sys.stdout.flush()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ── SHA-256 event ID ──────────────────────────────────────────────────────────
def make_event_ids(df):
    """Create a deterministic SHA-256 surrogate key per flight record."""
    keys = (
        df["flight_date"].astype(str)
        + "|" + df["airline"].astype(str)
        + "|" + df["flight_num"].astype(str)
        + "|" + df["origin"].astype(str)
        + "|" + df["dest"].astype(str)
        + "|" + df["crs_dep"].astype(str)
    )
    return keys.apply(lambda s: hashlib.sha256(s.encode()).hexdigest()[:16])

# ── 2400 rollover fix ─────────────────────────────────────────────────────────
def fix_2400(df):
    """
    BTS encodes midnight-next-day departures as 2400 (invalid HHMM).
    Convert: CRSDepTime 2400 → DepTimeFixed = 0000, FlightDate +1 day.
    """
    df["is_2400_rollover"] = False
    mask = pd.to_numeric(df["crs_dep"], errors="coerce") == 2400
    df.loc[mask, "is_2400_rollover"] = True
    df.loc[mask, "crs_dep"]          = 0
    df["dep_time_fixed"] = df["crs_dep"]  # copy after fix
    return df

# ── Quarantine rules ──────────────────────────────────────────────────────────
def classify_rows(df, essential_cols):
    """
    Returns (clean_mask, reasons_series).
    A row is quarantined if ANY quarantine condition is met.
    """
    reasons = pd.Series([""] * len(df), index=df.index)

    # R1 – missing essential key
    for col in essential_cols:
        if col in df.columns:
            bad = df[col].isna() | (df[col].astype(str).str.strip() == "")
            reasons[bad] = reasons[bad].str.cat(
                pd.Series([f"NULL:{col}"] * len(df), index=df.index)[bad], sep="|"
            )

    # R2 – CRSDepTime invalid (not 0-2359 or 2400)
    if "crs_dep" in df.columns:
        cd = pd.to_numeric(df["crs_dep"], errors="coerce")
        hr, mn = cd // 100, cd % 100
        bad_time = (cd < 0) | ((cd != 2400) & ((hr > 23) | (mn > 59))) | cd.isna()
        reasons[bad_time] = reasons[bad_time].str.cat(
            pd.Series(["INVALID_TIME:crs_dep"] * len(df), index=df.index)[bad_time], sep="|"
        )

    # R3 – arrival delay extreme  (< -180 or > 3600 minutes)
    if "arr_delay" in df.columns:
        ad = pd.to_numeric(df["arr_delay"], errors="coerce")
        bad_ad = (ad < -180) | (ad > 3600)
        reasons[bad_ad] = reasons[bad_ad].str.cat(
            pd.Series(["EXTREME:arr_delay"] * len(df), index=df.index)[bad_ad], sep="|"
        )

    # R4 – departure delay extreme
    if "dep_delay" in df.columns:
        dd = pd.to_numeric(df["dep_delay"], errors="coerce")
        bad_dd = (dd < -180) | (dd > 3600)
        reasons[bad_dd] = reasons[bad_dd].str.cat(
            pd.Series(["EXTREME:dep_delay"] * len(df), index=df.index)[bad_dd], sep="|"
        )

    reasons = reasons.str.lstrip("|")
    clean_mask = reasons == ""
    return clean_mask, reasons

# ── Per-month cleansing ───────────────────────────────────────────────────────
def cleanse_month(filepath, month):
    fname  = os.path.basename(filepath)
    log(f"Month {month:02d}: Starting cleanse - {fname}")

    # Read header to build alias map
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        raw_header = next(csv.reader(f))
    local_alias = {r: ALIAS[r] for r in raw_header if r in ALIAS}
    essential_std = []
    for std, candidates in ESSENTIAL.items():
        matched = [c for c in candidates if c in raw_header]
        if matched:
            essential_std.append(ALIAS.get(matched[0], matched[0]))

    # Output writers (opened once, appended chunk-by-chunk)
    clean_path  = os.path.join(CLEAN_CSV_DIR, f"clean_2025_{month:02d}.csv")
    quarq_path  = os.path.join(QUARANTINE,    f"quarantine_2025_{month:02d}.csv")
    parq_path   = os.path.join(PARQUET_DIR,   f"clean_2025_{month:02d}.parquet")

    total_rows = clean_rows = quarq_rows = rollover_rows = 0
    parquet_chunks = []
    first_chunk = True

    for chunk in pd.read_csv(
            filepath, chunksize=CHUNK_SIZE,
            low_memory=False, encoding="utf-8", on_bad_lines="skip"):

        chunk.rename(columns=local_alias, inplace=True)
        total_rows += len(chunk)

        # 2400 rollover fix (before quarantine so fixed rows are valid)
        chunk = fix_2400(chunk)
        rollover_rows += int(chunk["is_2400_rollover"].sum())

        # Quarantine classification
        clean_mask, reasons = classify_rows(chunk, essential_std)
        chunk["quarantine_reason"] = reasons

        # ── Clean subset ──────────────────────────────────────────────────────
        clean_df = chunk[clean_mask].copy()
        clean_df["quarantine_reason"] = ""
        clean_df["event_id"] = make_event_ids(clean_df)

        # Keep only standard columns that exist in this chunk
        out_cols = [c for c in KEEP_STD if c in clean_df.columns]
        clean_df = clean_df[out_cols]
        clean_rows += len(clean_df)

        # Append to clean CSV
        clean_df.to_csv(clean_path, mode="a", header=first_chunk, index=False)

        # Accumulate for parquet (write at end to avoid per-chunk parquet overhead)
        parquet_chunks.append(clean_df)

        # ── Quarantine subset ─────────────────────────────────────────────────
        bad_df = chunk[~clean_mask].copy()
        quarq_rows += len(bad_df)
        bad_df.to_csv(quarq_path, mode="a", header=first_chunk, index=False)

        first_chunk = False

    # Write consolidated parquet for this month
    if parquet_chunks:
        full_clean = pd.concat(parquet_chunks, ignore_index=True)
        full_clean.to_parquet(parq_path, index=False, compression="snappy")
        log(f"Month {month:02d}: Parquet written ({len(full_clean):,} rows) → {os.path.basename(parq_path)}")

    log(f"Month {month:02d}: DONE | Total={total_rows:,} | Clean={clean_rows:,} | "
        f"Quarantined={quarq_rows:,} | 2400-rollovers={rollover_rows:,}")

    return {
        "month": month,
        "source_file": fname,
        "total_rows":        total_rows,
        "clean_rows":        clean_rows,
        "quarantined_rows":  quarq_rows,
        "rollover_2400":     rollover_rows,
        "clean_rate_pct":    round(clean_rows / total_rows * 100, 4) if total_rows else 0,
        "clean_csv":         os.path.basename(clean_path),
        "parquet_file":      os.path.basename(parq_path),
        "quarantine_file":   os.path.basename(quarq_path),
    }

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log("=" * 72)
    log("STAGE 6 — Chunked Cleansing, Quarantine, 2400-Rollover Fix,")
    log("          SHA-256 Event IDs, Parquet + Clean-CSV Generation")
    log("=" * 72)
    results = []
    for m in range(1, 13):
        fp = os.path.join(CSV_DIR,
             f"On_Time_Reporting_Carrier_On_Time_Performance_2025_{m}.csv")
        if not os.path.exists(fp):
            log(f"MISSING: Month {m:02d} CSV — skipping"); continue
        results.append(cleanse_month(fp, m))

    df = pd.DataFrame(results)
    df.to_csv(CLEANSE_RPT, index=False)

    log("=" * 72)
    log(f"STAGE 6 COMPLETE")
    log(f"  Total rows processed : {df['total_rows'].sum():,}")
    log(f"  Total clean rows     : {df['clean_rows'].sum():,}")
    log(f"  Total quarantined    : {df['quarantined_rows'].sum():,}")
    log(f"  Total 2400 rollovers : {df['rollover_2400'].sum():,}")
    log(f"  Overall clean rate   : {df['clean_rows'].sum()/df['total_rows'].sum()*100:.4f}%")
    log(f"  Cleanse report       : {CLEANSE_RPT}")
    log("=" * 72)

if __name__ == "__main__":
    main()
