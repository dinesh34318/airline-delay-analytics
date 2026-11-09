# ==============================================================================
# scripts/download_manager.py
# High-Reliability BTS 2025 Downloader with Resume, Range Support & Manifest Tracking
# Supports selective month downloading (e.g., --months 1, --months 2-12)
# ==============================================================================

import os
import sys
import time
import hashlib
import zipfile
import argparse
import requests
from datetime import datetime

ZIP_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\dataset\raw\monthly_zip"
METADATA_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\dataset\metadata"
LOG_DIR = r"C:\Users\DELL\.gemini\antigravity\scratch\Airline-Delay-Analytics\logs"
MANIFEST_FILE = os.path.join(METADATA_DIR, "download_manifest.csv")
LOG_FILE = os.path.join(LOG_DIR, "download.log")

os.makedirs(ZIP_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

EXPECTED_SIZES = {
    1: 27108664,
    2: 25302583,
    3: 30544825,
    4: 29369186,
    5: 30830593,
    6: 31131411,
    7: 32208704,
    8: 30864245,
    9: 28151630,
    10: 30723803,
    11: 28847755,
    12: 30337431
}

BASE_URL = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2025_{}.zip"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*"
}

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)
    sys.stdout.flush()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def compute_sha256(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()

def test_zip_integrity(filepath):
    try:
        with zipfile.ZipFile(filepath, "r") as zf:
            bad_file = zf.testzip()
            if bad_file:
                return f"CORRUPTED_ENTRY_{bad_file}"
            namelist = zf.namelist()
            csv_files = [n for n in namelist if n.lower().endswith(".csv")]
            if not csv_files:
                return "NO_CSV_IN_ZIP"
            return "VALID"
    except zipfile.BadZipFile:
        return "BAD_ZIP_FILE"
    except Exception as e:
        return f"ERROR_{type(e).__name__}"

def update_manifest_row(month_res):
    """Update single row in download_manifest.csv preserving other rows"""
    rows = []
    header = "month,source_url,local_filename,http_status,content_length_bytes,file_size_bytes,sha256_checksum,zip_integrity_status,download_status,timestamp\n"
    
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if lines:
            header = lines[0]
            for line in lines[1:]:
                parts = line.strip().split(",")
                if parts and len(parts) >= 10:
                    try:
                        m_num = int(parts[0])
                        if m_num != month_res["month"]:
                            rows.append(line.strip())
                    except ValueError:
                        pass
    
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    new_line = f"{month_res['month']},{month_res['url']},{month_res['filename']},{month_res['http_status']},{month_res['content_length']},{month_res['file_size']},{month_res['sha256']},{month_res['zip_integrity']},{month_res['status']},{ts}"
    rows.append(new_line)
    
    # Sort by month
    def sort_key(x):
        try:
            return int(x.split(",")[0])
        except:
            return 99
    
    rows.sort(key=sort_key)
    
    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
        f.write(header)
        for r in rows:
            f.write(r + "\n")

def download_month(m, max_retries=5):
    url = BASE_URL.format(m)
    filename = f"On_Time_Reporting_Carrier_On_Time_Performance_2025_{m}.zip"
    filepath = os.path.join(ZIP_DIR, filename)
    expected_cl = EXPECTED_SIZES[m]
    
    # Check if already downloaded and completely valid
    if os.path.exists(filepath):
        cur_size = os.path.getsize(filepath)
        if cur_size == expected_cl:
            integrity = test_zip_integrity(filepath)
            if integrity == "VALID":
                log(f"Month {m:02d}: Already downloaded and valid ({cur_size:,} bytes). Calculating SHA-256...")
                sha = compute_sha256(filepath)
                res = {
                    "month": m,
                    "url": url,
                    "filename": filename,
                    "http_status": 200,
                    "content_length": expected_cl,
                    "file_size": cur_size,
                    "sha256": sha,
                    "zip_integrity": "VALID",
                    "status": "SUCCESS"
                }
                update_manifest_row(res)
                return res
            else:
                log(f"Month {m:02d}: Existing file corrupted ({integrity}). Removing and re-downloading...")
                os.remove(filepath)
        elif cur_size > expected_cl:
            log(f"Month {m:02d}: Existing file exceeds expected length ({cur_size} > {expected_cl}). Resetting...")
            os.remove(filepath)
    
    for attempt in range(1, max_retries + 1):
        try:
            req_headers = dict(HEADERS)
            existing_size = os.path.getsize(filepath) if os.path.exists(filepath) else 0
            
            if existing_size > 0 and existing_size < expected_cl:
                req_headers["Range"] = f"bytes={existing_size}-"
                mode = "ab"
                log(f"Month {m:02d}: Resuming from byte {existing_size:,} / {expected_cl:,} ({int(existing_size/expected_cl*100)}%) (Attempt {attempt}/{max_retries})...")
            else:
                mode = "wb"
                existing_size = 0
                log(f"Month {m:02d}: Downloading {expected_cl:,} bytes (Attempt {attempt}/{max_retries})...")
            
            resp = requests.get(url, headers=req_headers, stream=True, timeout=30)
            
            if resp.status_code in (200, 206):
                with open(filepath, mode) as f:
                    downloaded = existing_size
                    last_pct = -1
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            pct = int((downloaded / expected_cl) * 100)
                            if pct % 10 == 0 and pct != last_pct:
                                log(f"Month {m:02d}: Progress {downloaded:,} / {expected_cl:,} bytes ({pct}%)")
                                last_pct = pct
                
                final_size = os.path.getsize(filepath)
                if final_size == expected_cl:
                    integrity = test_zip_integrity(filepath)
                    if integrity == "VALID":
                        sha = compute_sha256(filepath)
                        log(f"Month {m:02d}: DOWNLOAD COMPLETE & VERIFIED. SHA-256: {sha}")
                        res = {
                            "month": m,
                            "url": url,
                            "filename": filename,
                            "http_status": resp.status_code,
                            "content_length": expected_cl,
                            "file_size": final_size,
                            "sha256": sha,
                            "zip_integrity": "VALID",
                            "status": "SUCCESS"
                        }
                        update_manifest_row(res)
                        return res
                    else:
                        log(f"Month {m:02d}: Integrity check failed: {integrity}. Retrying...")
                        os.remove(filepath)
                else:
                    log(f"Month {m:02d}: Size mismatch: {final_size} != {expected_cl}. Retrying...")
            else:
                log(f"Month {m:02d}: Unexpected HTTP status {resp.status_code}. Retrying...")
        
        except Exception as e:
            log(f"Month {m:02d}: Exception on attempt {attempt}: {type(e).__name__} ({e})")
            time.sleep(2 ** attempt)
    
    log(f"Month {m:02d}: FAILED after {max_retries} attempts.")
    res = {
        "month": m,
        "url": url,
        "filename": filename,
        "http_status": 500,
        "content_length": expected_cl,
        "file_size": os.path.getsize(filepath) if os.path.exists(filepath) else 0,
        "sha256": "FAILED",
        "zip_integrity": "FAILED",
        "status": "FAILED"
    }
    update_manifest_row(res)
    return res

def parse_months(arg_str):
    if not arg_str:
        return list(range(1, 13))
    if "-" in arg_str:
        start, end = arg_str.split("-")
        return list(range(int(start), int(end) + 1))
    if "," in arg_str:
        return [int(x.strip()) for x in arg_str.split(",")]
    return [int(arg_str.strip())]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=str, default="1", help="Months to download (e.g. 1, 1-12, 2-12)")
    args = parser.parse_args()
    
    target_months = parse_months(args.months)
    
    log("=" * 70)
    log(f"Executing BTS 2025 Download for Month(s): {target_months}")
    log("=" * 70)
    
    results = []
    all_success = True
    
    for m in target_months:
        res = download_month(m)
        results.append(res)
        if res["status"] != "SUCCESS":
            all_success = False
            break # Stop on failure
    
    log("=" * 70)
    log(f"BATCH RUN FINISHED. Manifest updated: {MANIFEST_FILE}")
    for r in results:
        log(f"Month {r['month']:02d}: Status={r['status']}, Size={r['file_size']:,} bytes, Integrity={r['zip_integrity']}, SHA256={r['sha256'][:16]}...")
    log("=" * 70)

if __name__ == "__main__":
    main()
