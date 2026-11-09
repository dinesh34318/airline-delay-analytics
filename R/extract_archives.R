# ==============================================================================
# R/extract_archives.R
# Pipeline Stage 3: Safe Archive Extraction & Path Traversal Guards
# Strict Architecture: R 4.x (digest, logger)
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("digest", quietly = TRUE)) install.packages("digest", repos = "https://cloud.r-project.org")
  if (!requireNamespace("logger", quietly = TRUE)) install.packages("logger", repos = "https://cloud.r-project.org")
  library(digest)
  library(logger)
})

log_file <- file.path("logs", "extraction.log")
dir.create("logs", showWarnings = FALSE, recursive = TRUE)
log_appender(appender_tee(log_file))
log_info("Initializing Archive Extraction Engine (Stage 3)")

ZIP_DIR <- file.path("dataset", "raw", "monthly_zip")
CSV_DIR <- file.path("dataset", "raw", "monthly_csv")
dir.create(CSV_DIR, showWarnings = FALSE, recursive = TRUE)

#' Guard against Zip Slip path traversal vulnerabilities
validate_safe_entry <- function(entry_name, dest_dir) {
  # Disallow absolute paths and directory traversal
  if (grepl("^(/|\\\\|[a-zA-Z]:)", entry_name)) {
    stop(sprintf("Zip Slip security violation: Absolute path in archive entry '%s'", entry_name))
  }
  if (grepl("\\.\\.", entry_name)) {
    stop(sprintf("Zip Slip security violation: Directory traversal (..) in archive entry '%s'", entry_name))
  }
  
  target_path <- normalizePath(file.path(dest_dir, entry_name), mustWork = FALSE)
  dest_normalized <- normalizePath(dest_dir, mustWork = TRUE)
  
  if (!startsWith(target_path, dest_normalized)) {
    stop(sprintf("Zip Slip security violation: Entry '%s' resolves outside destination '%s'", entry_name, dest_dir))
  }
  TRUE
}

extract_single_archive <- function(zip_path, dest_dir = CSV_DIR) {
  zip_name <- basename(zip_path)
  log_info("Inspecting archive: {zip_name}")
  
  entries <- utils::unzip(zip_path, list = TRUE)
  if (nrow(entries) == 0) {
    log_error("Archive {zip_name} is empty.")
    return(NULL)
  }
  
  # Audit every archive member
  for (name in entries$Name) {
    validate_safe_entry(name, dest_dir)
  }
  
  # Find target flight CSV
  csv_entries <- entries[grepl("\\.csv$", entries$Name, ignore.case = TRUE), ]
  if (nrow(csv_entries) == 0) {
    log_error("No CSV files found inside {zip_name}")
    return(NULL)
  }
  
  # Identify primary monthly dataset entry (usually largest CSV or matches On_Time naming)
  primary_csv_entry <- csv_entries$Name[which.max(csv_entries$Length)]
  log_info("Extracting primary dataset member '{primary_csv_entry}' ({max(csv_entries$Length)} bytes)...")
  
  # Safe extraction of the primary CSV
  utils::unzip(zip_path, files = primary_csv_entry, exdir = dest_dir, overwrite = TRUE)
  extracted_raw_path <- file.path(dest_dir, primary_csv_entry)
  
  # Normalize target filename
  # Extract year and month from zip filename
  m_matches <- regmatches(zip_name, regexec("([0-9]{4})_([0-9]{1,2})", zip_name))[[1]]
  if (length(m_matches) >= 3) {
    year_val <- m_matches[2]
    month_val <- m_matches[3]
    normalized_name <- sprintf("On_Time_Reporting_Carrier_On_Time_Performance_%s_%s.csv", year_val, month_val)
    normalized_path <- file.path(dest_dir, normalized_name)
    
    if (extracted_raw_path != normalized_path) {
      file.rename(extracted_raw_path, normalized_path)
      log_info("Normalized file name: '{primary_csv_entry}' -> '{normalized_name}'")
    }
    final_path <- normalized_path
  } else {
    final_path <- extracted_raw_path
  }
  
  sz <- file.info(final_path)$size
  log_info("Extraction complete: {basename(final_path)} ({sz} bytes)")
  
  data.frame(
    archive = zip_name,
    internal_entry = primary_csv_entry,
    extracted_file = basename(final_path),
    size_bytes = sz,
    timestamp = as.character(Sys.time()),
    stringsAsFactors = FALSE
  )
}

execute_archive_extraction_pipeline <- function() {
  zip_files <- sort(list.files(ZIP_DIR, pattern = "\\.zip$", full.names = TRUE))
  if (length(zip_files) == 0) {
    log_warn("No ZIP archives found in {ZIP_DIR} to extract.")
    return(invisible(NULL))
  }
  
  log_info("Found {length(zip_files)} archives to extract in {ZIP_DIR}")
  records <- list()
  for (zf in zip_files) {
    res <- extract_single_archive(zf)
    if (!is.null(res)) records[[length(records) + 1]] <- res
  }
  
  if (length(records) > 0) {
    df <- do.call(rbind, records)
    out_meta <- file.path("dataset", "metadata", "extraction_manifest.csv")
    write.csv(df, out_meta, row.names = FALSE)
    log_info("Extraction manifest recorded at: {out_meta}")
    print(df)
  }
  invisible(records)
}

if (!interactive()) {
  execute_archive_extraction_pipeline()
}
