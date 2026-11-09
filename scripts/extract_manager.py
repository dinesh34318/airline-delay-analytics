# ==============================================================================
# scripts/extract_manager.py
# Stage 3: Safe Archive Extraction Engine with Anti-Zip Slip Security Guards
# ==============================================================================

import os
import sys
import zipfile
from datetime import datetime

ZIP_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\dataset\raw\monthly_zip"
CSV_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\dataset\raw\monthly_csv"
METADATA_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\dataset\metadata"
LOG_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\logs"

os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

EXTRACTION_MANIFEST = os.path.join(METADATA_DIR, "extraction_manifest.csv")
LOG_FILE = os.path.join(LOG_DIR, "extraction.log")

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)
    sys.stdout.flush()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def is_safe_path(base_dir, path, follow_symlinks=True):
    # Prevent Zip Slip / path traversal
    matchpath = os.path.realpath(path) if follow_symlinks else os.path.abspath(path)
    return base_dir == os.path.commonpath((base_dir, matchpath))

def extract_month_archive(zip_path, month):
    zip_name = os.path.basename(zip_path)
    log(f"Month {month:02d}: Inspecting archive {zip_name}...")
    
    with zipfile.ZipFile(zip_path, "r") as zf:
        namelist = zf.namelist()
        
        # Security validation for all members
        for member in namelist:
            target_dest = os.path.join(CSV_DIR, member)
            if not is_safe_path(CSV_DIR, target_dest):
                raise SecurityError(f"Zip Slip detected! Malicious path '{member}' in {zip_name}")
        
        # Identify primary CSV
        csv_members = [m for m in namelist if m.lower().endswith(".csv")]
        if not csv_members:
            raise ValueError(f"No CSV files found inside {zip_name}")
        
        # Take the largest CSV member (primary dataset)
        primary_csv = max(csv_members, key=lambda m: zf.getinfo(m).file_size)
        uncompressed_size = zf.getinfo(primary_csv).file_size
        
        normalized_filename = f"On_Time_Reporting_Carrier_On_Time_Performance_2025_{month}.csv"
        normalized_path = os.path.join(CSV_DIR, normalized_filename)
        
        log(f"Month {month:02d}: Extracting '{primary_csv}' ({uncompressed_size:,} bytes / {uncompressed_size / (1024**2):.2f} MB)...")
        
        # Stream extract to target normalized filename
        with zf.open(primary_csv) as source, open(normalized_path, "wb") as target:
            while chunk := source.read(1024 * 1024):
                target.write(chunk)
        
        extracted_actual_size = os.path.getsize(normalized_path)
        if extracted_actual_size != uncompressed_size:
            raise IOError(f"Size mismatch during extraction: {extracted_actual_size} != {uncompressed_size}")
        
        log(f"Month {month:02d}: Extraction verified -> {normalized_filename} ({extracted_actual_size:,} bytes)")
        
        return {
            "month": month,
            "archive_name": zip_name,
            "internal_entry": primary_csv,
            "extracted_csv": normalized_filename,
            "uncompressed_bytes": extracted_actual_size,
            "status": "VALID_EXTRACTED",
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        }

def main():
    log("=" * 70)
    log("Starting Stage 3: Safe Archive Extraction (All 12 Months)")
    log("=" * 70)
    
    results = []
    total_uncompressed = 0
    all_success = True
    
    for m in range(1, 13):
        zip_filename = f"On_Time_Reporting_Carrier_On_Time_Performance_2025_{m}.zip"
        zip_path = os.path.join(ZIP_DIR, zip_filename)
        
        if not os.path.exists(zip_path):
            log(f"ERROR: Missing archive for month {m}: {zip_path}")
            all_success = False
            continue
        
        try:
            res = extract_month_archive(zip_path, m)
            results.append(res)
            total_uncompressed += res["uncompressed_bytes"]
        except Exception as e:
            log(f"ERROR extracting month {m}: {type(e).__name__} ({e})")
            all_success = False
    
    # Write extraction manifest
    with open(EXTRACTION_MANIFEST, "w", encoding="utf-8") as f:
        f.write("month,archive_name,internal_entry,extracted_csv,uncompressed_bytes,status,timestamp\n")
        for r in results:
            f.write(f"{r['month']},{r['archive_name']},{r['internal_entry']},{r['extracted_csv']},{r['uncompressed_bytes']},{r['status']},{r['timestamp']}\n")
    
    log("=" * 70)
    log(f"STAGE 3 COMPLETE. Manifest written: {EXTRACTION_MANIFEST}")
    log(f"Total Uncompressed Volume: {total_uncompressed:,} bytes ({total_uncompressed / (1024**2):.2f} MB / {total_uncompressed / (1024**3):.2f} GB)")
    log(f"Overall Status: {'ALL 12 MONTHS SAFELY EXTRACTED' if all_success else 'ERRORS ENCOUNTERED'}")
    log("=" * 70)

if __name__ == "__main__":
    main()
