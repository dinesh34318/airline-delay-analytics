# ==============================================================================
# R/inspect_dataset.R
# Pipeline Stage 4: Dataset Profiler, Schema Mapping & Data Dictionary Generator
# Strict Architecture: R 4.x (data.table, logger)
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table", repos = "https://cloud.r-project.org")
  if (!requireNamespace("logger", quietly = TRUE)) install.packages("logger", repos = "https://cloud.r-project.org")
  library(data.table)
  library(logger)
})

log_file <- file.path("logs", "inspection.log")
dir.create("logs", showWarnings = FALSE, recursive = TRUE)
log_appender(appender_tee(log_file))
log_info("Initializing Dataset Inspection Engine (Stage 4)")

CSV_DIR <- file.path("dataset", "raw", "monthly_csv")
METADATA_DIR <- file.path("dataset", "metadata")
DICT_FILE <- file.path(METADATA_DIR, "dataset_dictionary.csv")
MONTHLY_SUMMARY_FILE <- file.path(METADATA_DIR, "monthly_summary.csv")

# Standardized column mapping specifications
COLUMN_MAPPINGS <- list(
  flight_date = c("FlightDate", "FL_DATE"),
  airline     = c("Reporting_Airline", "OP_UNIQUE_CARRIER", "UniqueCarrier", "DOT_ID_Reporting_Airline"),
  flight_num  = c("Flight_Number_Reporting_Airline", "OP_CARRIER_FL_NUM", "FlightNum"),
  origin      = c("Origin", "ORIGIN"),
  dest        = c("Dest", "DEST"),
  crs_dep     = c("CRSDepTime", "CRS_DEP_TIME"),
  dep_delay   = c("DepDelay", "DEP_DELAY"),
  arr_delay   = c("ArrDelay", "ARR_DELAY"),
  cancelled   = c("Cancelled", "CANCELLED"),
  diverted    = c("Diverted", "DIVERTED")
)

resolve_column_names <- function(header_names) {
  resolved <- list()
  for (std_name in names(COLUMN_MAPPINGS)) {
    candidates <- COLUMN_MAPPINGS[[std_name]]
    matched <- intersect(candidates, header_names)
    if (length(matched) > 0) {
      resolved[[std_name]] <- matched[1]
    } else {
      resolved[[std_name]] <- NA_character_
    }
  }
  resolved
}

inspect_monthly_csv <- function(csv_path) {
  fname <- basename(csv_path)
  log_info("Profiling CSV file: {fname}")
  
  # Read header only first
  header <- names(fread(csv_path, nrows = 0))
  resolved_map <- resolve_column_names(header)
  
  missing_required <- names(resolved_map)[is.na(unlist(resolved_map))]
  if (length(missing_required) > 0) {
    log_error("File {fname} is missing required standard columns: {paste(missing_required, collapse=', ')}")
  }
  
  # Sample 10,000 rows for type and missingness profiling
  sample_dt <- fread(csv_path, nrows = 10000, select = na.omit(unlist(resolved_map)))
  
  # Rename sample columns to standard names
  rev_map <- setNames(names(resolved_map), unlist(resolved_map))
  setnames(sample_dt, old = names(sample_dt), new = rev_map[names(sample_dt)])
  
  # Count total rows safely
  total_rows <- as.numeric(system(sprintf("wc -l < \"%s\"", normalizePath(csv_path)), intern = TRUE)) - 1
  if (is.na(total_rows) || total_rows < 0) {
    # Fallback to pure R count if wc -l fails on Windows
    total_rows <- nrow(fread(csv_path, select = 1L))
  }
  
  file_sz <- file.info(csv_path)$size
  
  summary_info <- list(
    filename = fname,
    size_bytes = file_sz,
    total_rows = total_rows,
    total_columns_in_source = length(header),
    sample_rows = nrow(sample_dt),
    missing_flight_date = sum(is.na(sample_dt$flight_date)),
    missing_airline = sum(is.na(sample_dt$airline)),
    missing_flight_num = sum(is.na(sample_dt$flight_num)),
    missing_origin = sum(is.na(sample_dt$origin)),
    missing_dest = sum(is.na(sample_dt$dest)),
    missing_crs_dep = sum(is.na(sample_dt$crs_dep)),
    missing_dep_delay = sum(is.na(sample_dt$dep_delay)),
    missing_arr_delay = sum(is.na(sample_dt$arr_delay)),
    cancelled_count = sum(sample_dt$cancelled == 1, na.rm = TRUE),
    diverted_count = sum(sample_dt$diverted == 1, na.rm = TRUE)
  )
  
  log_info("Finished profiling {fname}: {total_rows} records, {length(header)} raw columns")
  list(summary = summary_info, resolved = resolved_map)
}

generate_dataset_dictionary <- function() {
  dict_df <- data.frame(
    standard_column = c(
      "flight_date", "airline", "flight_num", "origin", "dest",
      "crs_dep", "dep_delay", "arr_delay", "cancelled", "diverted",
      "sched_dep_hour", "sched_dep_local", "sched_dep_utc",
      "event_id", "is_valid_arr", "is_valid_dep", "source_file", "source_row_num", "raw_row_hash"
    ),
    target_data_type = c(
      "STRING (Date YYYY-MM-DD)", "STRING (IATA/DOT)", "INT32", "STRING (IATA)", "STRING (IATA)",
      "INT32 (HHMM)", "DOUBLE (Minutes)", "DOUBLE (Minutes)", "INT32 (0/1)", "INT32 (0/1)",
      "INT32 (0-23)", "TIMESTAMP (Local Airport)", "TIMESTAMP (UTC Synchronized)",
      "STRING (SHA-256)", "INT32 (0/1)", "INT32 (0/1)", "STRING", "INT64", "STRING (SHA-256)"
    ),
    canonical_bts_name = c(
      "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline", "Origin", "Dest",
      "CRSDepTime", "DepDelay", "ArrDelay", "Cancelled", "Diverted",
      "DERIVED (floor(CRSDepTime/100))", "DERIVED (Date + CRSDepTime)", "DERIVED (Local to UTC via IANA)",
      "DERIVED (SHA-256 hash)", "DERIVED (Flag)", "DERIVED (Flag)", "METADATA", "METADATA", "METADATA (SHA-256 of raw CSV line)"
    ),
    role_in_research = c(
      "Partitioning key & temporal alignment",
      "Workload W1 primary grouping key",
      "Key component for event_id uniqueness",
      "Workload W2 origin airport grouping, W4 route key",
      "Workload W4 destination airport grouping",
      "Scheduled departure time (local military format)",
      "Departure delay measure for Workload W2",
      "Arrival delay measure for Workloads W1, W3, W4",
      "Cancellation metric for Workload W1",
      "Routing and flight validity filtering",
      "Scheduled departure hour (0-23) for Workload W2",
      "Event timestamp for local diurnal congestion windowing",
      "Event timestamp for UTC synchronized stream replay",
      "Unique flight event key for stream deduplication & fault injection",
      "Valid arrival delay indicator flag (zero imputation policy)",
      "Valid departure delay indicator flag (zero imputation policy)",
      "Source traceability audit field",
      "Source line number audit field",
      "Cryptographic raw data integrity hash"
    ),
    stringsAsFactors = FALSE
  )
  write.csv(dict_df, DICT_FILE, row.names = FALSE)
  log_info("Dataset dictionary generated at: {DICT_FILE}")
}

execute_dataset_inspection_pipeline <- function() {
  csv_files <- sort(list.files(CSV_DIR, pattern = "\\.csv$", full.names = TRUE))
  if (length(csv_files) == 0) {
    log_warn("No CSV files found in {CSV_DIR} to inspect.")
    return(invisible(NULL))
  }
  
  summaries <- list()
  for (cf in csv_files) {
    res <- inspect_monthly_csv(cf)
    summaries[[length(summaries) + 1]] <- as.data.frame(res$summary)
  }
  
  sum_df <- do.call(rbind, summaries)
  write.csv(sum_df, MONTHLY_SUMMARY_FILE, row.names = FALSE)
  log_info("Monthly inspection summary saved to: {MONTHLY_SUMMARY_FILE}")
  
  generate_dataset_dictionary()
  print(sum_df)
  invisible(sum_df)
}

if (!interactive()) {
  execute_dataset_inspection_pipeline()
}
