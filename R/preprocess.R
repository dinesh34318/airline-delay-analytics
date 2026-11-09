# ==============================================================================
# R/preprocess.R
# Pipeline Stage 6: Chunked Cleansing, Quarantine, Flags, & Parquet/CSV Generator
# Strict Architecture: R 4.x (data.table, arrow, lubridate, digest, logger)
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table", repos = "https://cloud.r-project.org")
  if (!requireNamespace("arrow", quietly = TRUE)) install.packages("arrow", repos = "https://cloud.r-project.org")
  if (!requireNamespace("lubridate", quietly = TRUE)) install.packages("lubridate", repos = "https://cloud.r-project.org")
  if (!requireNamespace("digest", quietly = TRUE)) install.packages("digest", repos = "https://cloud.r-project.org")
  if (!requireNamespace("jsonlite", quietly = TRUE)) install.packages("jsonlite", repos = "https://cloud.r-project.org")
  if (!requireNamespace("logger", quietly = TRUE)) install.packages("logger", repos = "https://cloud.r-project.org")
  library(data.table)
  library(arrow)
  library(lubridate)
  library(digest)
  library(jsonlite)
  library(logger)
})

log_file <- file.path("logs", "preprocessing_log.txt")
dir.create("logs", showWarnings = FALSE, recursive = TRUE)
log_appender(appender_tee(log_file))
log_info("Initializing Preprocessing & Cleansing Engine (Stage 6)")

CSV_DIR <- file.path("dataset", "raw", "monthly_csv")
OUT_PARQUET_DIR <- file.path("dataset", "processed", "parquet")
OUT_CSV_DIR <- file.path("dataset", "processed", "csv")
METADATA_DIR <- file.path("dataset", "metadata")
QUARANTINE_FILE <- file.path(METADATA_DIR, "quarantine_records.csv")
MEMORY_LOG_FILE <- file.path("logs", "preprocessing_memory_profile.csv")

dir.create(OUT_PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(OUT_CSV_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(METADATA_DIR, showWarnings = FALSE, recursive = TRUE)

# Record memory usage telemetry
log_memory_telemetry <- function(phase_name) {
  gc_res <- gc(full = TRUE)
  # gc_res returns: Ncells, Vcells (used, gc trigger, max used, etc.)
  used_mb <- sum(gc_res[, 2] * c(node_size = 56, byte_size = 8)) / (1024^2)
  max_mb <- sum(gc_res[, 6] * c(node_size = 56, byte_size = 8)) / (1024^2)
  
  entry <- data.frame(
    timestamp = as.character(Sys.time()),
    phase = phase_name,
    used_ram_mb = round(used_mb, 2),
    peak_ram_mb = round(max_mb, 2),
    stringsAsFactors = FALSE
  )
  
  if (!file.exists(MEMORY_LOG_FILE)) {
    write.csv(entry, MEMORY_LOG_FILE, row.names = FALSE)
  } else {
    write.table(entry, MEMORY_LOG_FILE, sep = ",", row.names = FALSE, col.names = FALSE, append = TRUE)
  }
  log_info("Memory Telemetry [{phase_name}]: Used = {round(used_mb, 1)} MB, Peak = {round(max_mb, 1)} MB")
}

# Source column resolution function
source("R/inspect_dataset.R")

# Vectorized SHA-256 hashing for event_id
hash_vector_sha256 <- function(vec) {
  vapply(vec, function(x) digest(x, algo = "sha256", serialize = FALSE), FUN.VALUE = character(1), USE.NAMES = FALSE)
}

#' Process a single monthly raw CSV file
process_monthly_file <- function(raw_csv_path) {
  fname <- basename(raw_csv_path)
  log_info("Processing month file: {fname}")
  log_memory_telemetry(paste0("Start_", fname))
  
  header <- names(fread(raw_csv_path, nrows = 0))
  resolved_map <- resolve_column_names(header)
  selected_cols <- na.omit(unlist(resolved_map))
  
  # Read full monthly file via data.table (optimized fast C parser)
  dt <- fread(raw_csv_path, select = selected_cols, colClasses = list(character = c("FlightDate", "Reporting_Airline", "Origin", "Dest", "FL_DATE", "OP_UNIQUE_CARRIER")))
  rev_map <- setNames(names(resolved_map), unlist(resolved_map))
  setnames(dt, old = names(dt), new = rev_map[names(dt)])
  
  n_raw <- nrow(dt)
  dt[, source_file := fname]
  dt[, source_row_num := .I]
  
  # 1. Essential Key Validation & Quarantine
  missing_mask <- (
    is.na(dt$flight_date) | dt$flight_date == "" |
    is.na(dt$airline) | dt$airline == "" |
    is.na(dt$flight_num) |
    is.na(dt$origin) | dt$origin == "" |
    is.na(dt$dest) | dt$dest == "" |
    is.na(dt$crs_dep) |
    is.na(dt$cancelled)
  )
  
  # 2. Scheduled Time Validation
  hour_val <- dt$crs_dep %/% 100
  min_val  <- dt$crs_dep %% 100
  time_corrupt_mask <- (
    !missing_mask &
    (dt$crs_dep < 0 | (dt$crs_dep != 2400 & (hour_val < 0 | hour_val > 23 | min_val < 0 | min_val > 59)))
  )
  
  quarantine_mask <- missing_mask | time_corrupt_mask
  
  # Extract quarantined rows
  if (any(quarantine_mask)) {
    dt_quarantine <- dt[quarantine_mask]
    dt_quarantine[, quarantine_reason := fifelse(missing_mask[quarantine_mask], "MISSING_ESSENTIAL_KEY", "INVALID_SCHEDULED_TIME")]
    
    # Write to quarantine file
    if (!file.exists(QUARANTINE_FILE)) {
      fwrite(dt_quarantine, QUARANTINE_FILE)
    } else {
      fwrite(dt_quarantine, QUARANTINE_FILE, append = TRUE)
    }
    log_warn("Quarantined {nrow(dt_quarantine)} invalid records from {fname}")
  } else {
    dt_quarantine <- dt[0]
  }
  
  # Filter retained candidate rows
  dt_clean <- dt[!quarantine_mask]
  rm(dt)
  gc()
  
  # 3. Scheduled Time Conversion and 2400 Rollover
  # Handle 2400 midnight rollover to next day 0000
  dt_clean[, is_2400 := (crs_dep == 2400)]
  dt_clean[, sched_dep_hour := fifelse(is_2400, 0L, as.integer(crs_dep %/% 100))]
  dt_clean[, sched_dep_min  := fifelse(is_2400, 0L, as.integer(crs_dep %% 100))]
  
  # Compute local scheduled departure timestamp
  base_date <- as.Date(dt_clean$flight_date)
  actual_date <- fifelse(dt_clean$is_2400, base_date + 1, base_date)
  
  dt_clean[, sched_dep_local := as.POSIXct(actual_date) + (sched_dep_hour * 3600) + (sched_dep_min * 60)]
  dt_clean[, is_2400 := NULL]
  dt_clean[, sched_dep_min := NULL]
  
  # 4. Deterministic Structured Flight Key & Event ID
  # Structured key: flight_date|airline|flight_num|origin|dest|crs_dep
  dt_clean[, structured_key := paste(
    flight_date, airline, as.character(flight_num), origin, dest, sprintf("%04d", crs_dep), sep = "|"
  )]
  
  # 5. Duplicate Classification
  # Count occurrences of structured_key
  dt_clean[, key_count := .N, by = structured_key]
  
  # Separate unique from duplicate keys
  dt_unique <- dt_clean[key_count == 1]
  dt_dups   <- dt_clean[key_count > 1]
  
  exact_dups_removed <- 0L
  conflicting_dups_count <- 0L
  
  if (nrow(dt_dups) > 0) {
    # Check if all duplicate instances share identical delay, cancellation, and diversion status
    dt_dups[, is_exact_match := (
      uniqueN(arr_delay) == 1 &
      uniqueN(dep_delay) == 1 &
      uniqueN(cancelled) == 1 &
      uniqueN(diverted) == 1
    ), by = structured_key]
    
    # Exact duplicates: keep first instance, drop remainder
    exact_matches <- dt_dups[is_exact_match == TRUE]
    exact_kept <- exact_matches[, .SD[1], by = structured_key]
    exact_dropped <- exact_matches[, .SD[-1], by = structured_key]
    exact_dups_removed <- nrow(exact_dropped)
    
    # Conflicting duplicates: quarantine all occurrences
    conflicting_matches <- dt_dups[is_exact_match == FALSE]
    conflicting_dups_count <- nrow(conflicting_matches)
    
    if (conflicting_dups_count > 0) {
      conflicting_matches[, quarantine_reason := "CONFLICTING_KEY_DUPLICATE"]
      fwrite(conflicting_matches, QUARANTINE_FILE, append = TRUE)
      log_warn("Quarantined {conflicting_dups_count} conflicting duplicate records from {fname}")
    }
    
    dt_retained <- rbind(dt_unique, exact_kept, fill = TRUE)
  } else {
    dt_retained <- dt_unique
  }
  
  rm(dt_clean, dt_unique, dt_dups)
  gc()
  
  dt_retained[, key_count := NULL]
  if ("is_exact_match" %in% names(dt_retained)) dt_retained[, is_exact_match := NULL]
  
  # Generate SHA-256 event_id for retained records
  dt_retained[, event_id := hash_vector_sha256(structured_key)]
  dt_retained[, structured_key := NULL]
  
  # 6. Zero-Imputation Policy & Delay Validity Flags
  dt_retained[, diverted := fifelse(is.na(diverted), 0L, as.integer(diverted))]
  dt_retained[, cancelled := as.integer(cancelled)]
  dt_retained[, flight_num := as.integer(flight_num)]
  dt_retained[, crs_dep := as.integer(crs_dep)]
  dt_retained[, dep_delay := as.numeric(dep_delay)]
  dt_retained[, arr_delay := as.numeric(arr_delay)]
  
  dt_retained[, is_valid_arr := as.integer(cancelled == 0L & diverted == 0L & !is.na(arr_delay))]
  dt_retained[, is_valid_dep := as.integer(cancelled == 0L & !is.na(dep_delay))]
  
  n_retained <- nrow(dt_retained)
  n_quarantine_total <- nrow(dt_quarantine) + conflicting_dups_count
  
  # 7. Assert Monthly Row Conservation Equation
  row_conservation_verified <- (n_raw == (n_retained + n_quarantine_total + exact_dups_removed))
  if (!row_conservation_verified) {
    stop(sprintf("Row conservation assertion failed for %s: Raw(%d) != Retained(%d) + Quarantine(%d) + ExactDups(%d)",
                 fname, n_raw, n_retained, n_quarantine_total, exact_dups_removed))
  }
  log_info("Row Conservation Verified for {fname}: {n_raw} = {n_retained} + {n_quarantine_total} + {exact_dups_removed}")
  
  # 8. Sort chronologically by scheduled departure time
  setorder(dt_retained, sched_dep_local)
  
  # 9. Export Cleaned Monthly Parquet & CSV
  base_out_name <- sub("\\.csv$", "", fname)
  out_parquet_path <- file.path(OUT_PARQUET_DIR, paste0(base_out_name, ".parquet"))
  out_csv_path <- file.path(OUT_CSV_DIR, paste0(base_out_name, ".csv"))
  
  # Parquet format compatible with Hive 1.2.1 (INT96 timestamps)
  arrow::write_parquet(
    dt_retained,
    out_parquet_path,
    compression = "snappy",
    use_deprecated_int96_timestamps = TRUE
  )
  fwrite(dt_retained, out_csv_path)
  
  log_info("Exported cleaned datasets for {fname}: Parquet ({file.info(out_parquet_path)$size} bytes), CSV ({file.info(out_csv_path)$size} bytes)")
  log_memory_telemetry(paste0("End_", fname))
  
  list(
    filename = fname,
    raw_rows = n_raw,
    retained_rows = n_retained,
    quarantine_rows = n_quarantine_total,
    exact_duplicates_removed = exact_dups_removed,
    row_conservation_verified = row_conservation_verified,
    parquet_path = out_parquet_path,
    csv_path = out_csv_path
  )
}

execute_preprocessing_pipeline <- function() {
  csv_files <- sort(list.files(CSV_DIR, pattern = "\\.csv$", full.names = TRUE))
  if (length(csv_files) == 0) {
    log_warn("No CSV files found in {CSV_DIR} to preprocess.")
    return(invisible(NULL))
  }
  
  log_info("Starting batch preprocessing across {length(csv_files)} monthly files...")
  monthly_results <- list()
  
  for (cf in csv_files) {
    monthly_results[[length(monthly_results) + 1]] <- process_monthly_file(cf)
  }
  
  # Compile preprocessing summary JSON
  summary_json_path <- file.path(METADATA_DIR, "preprocessing_summary.json")
  summary_data <- list(
    pipeline_version = "3.1",
    execution_time = as.character(Sys.time()),
    months_processed = length(monthly_results),
    total_raw_rows = sum(vapply(monthly_results, function(x) x$raw_rows, numeric(1))),
    total_retained_rows = sum(vapply(monthly_results, function(x) x$retained_rows, numeric(1))),
    total_quarantine_rows = sum(vapply(monthly_results, function(x) x$quarantine_rows, numeric(1))),
    total_duplicates_removed = sum(vapply(monthly_results, function(x) x$exact_duplicates_removed, numeric(1))),
    monthly_breakdown = monthly_results
  )
  
  write_json(summary_data, summary_json_path, pretty = TRUE, auto_unbox = TRUE)
  log_info("Preprocessing execution summary JSON saved to: {summary_json_path}")
  invisible(summary_data)
}

if (!interactive()) {
  execute_preprocessing_pipeline()
}
