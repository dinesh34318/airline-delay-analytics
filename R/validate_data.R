# ==============================================================================
# R/validate_data.R
# Pipeline Stage 4/5: Comprehensive Data Quality Auditor & Anomaly Detector
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
log_info("Initializing Data Quality Validation Engine")

CSV_DIR <- file.path("dataset", "raw", "monthly_csv")
METADATA_DIR <- file.path("dataset", "metadata")
QUALITY_REPORT_FILE <- file.path(METADATA_DIR, "data_quality_report.csv")

audit_csv_quality <- function(csv_path) {
  fname <- basename(csv_path)
  log_info("Running comprehensive quality audit on: {fname}")
  
  # Map columns
  header <- names(fread(csv_path, nrows = 0))
  source "R/inspect_dataset.R"
  resolved_map <- resolve_column_names(header)
  
  selected_cols <- na.omit(unlist(resolved_map))
  dt <- fread(csv_path, select = selected_cols)
  
  rev_map <- setNames(names(resolved_map), unlist(resolved_map))
  setnames(dt, old = names(dt), new = rev_map[names(dt)])
  
  n_total <- nrow(dt)
  
  # 1. Missing Essential Identifiers
  n_missing_date <- sum(is.na(dt$flight_date) | dt$flight_date == "")
  n_missing_airline <- sum(is.na(dt$airline) | dt$airline == "")
  n_missing_flight_num <- sum(is.na(dt$flight_num))
  n_missing_origin <- sum(is.na(dt$origin) | dt$origin == "")
  n_missing_dest <- sum(is.na(dt$dest) | dt$dest == "")
  n_missing_crs_dep <- sum(is.na(dt$crs_dep))
  n_missing_cancelled <- sum(is.na(dt$cancelled))
  
  # 2. Time validity check
  hour_val <- dt$crs_dep %/% 100
  min_val  <- dt$crs_dep %% 100
  n_invalid_time <- sum(
    is.na(dt$crs_dep) |
    dt$crs_dep < 0 |
    (dt$crs_dep != 2400 & (hour_val < 0 | hour_val > 23 | min_val < 0 | min_val > 59)),
    na.rm = TRUE
  )
  n_rollover_2400 <- sum(dt$crs_dep == 2400, na.rm = TRUE)
  
  # 3. Delay Range Audit (Log extreme values, never drop)
  n_arr_delay_outside_bounds <- sum(!is.na(dt$arr_delay) & (dt$arr_delay < -120 | dt$arr_delay > 3000))
  n_dep_delay_outside_bounds <- sum(!is.na(dt$dep_delay) & (dt$dep_delay < -120 | dt$dep_delay > 3000))
  
  # 4. Flight Status Counts
  n_cancelled <- sum(dt$cancelled == 1, na.rm = TRUE)
  n_diverted <- sum(dt$diverted == 1, na.rm = TRUE)
  n_valid_arr <- sum(dt$cancelled == 0 & dt$diverted == 0 & !is.na(dt$arr_delay))
  n_valid_dep <- sum(dt$cancelled == 0 & !is.na(dt$dep_delay))
  
  # 5. Delay Imputation Audit (Verify cancelled flights have NA delays)
  n_cancelled_with_arr_delay <- sum(dt$cancelled == 1 & !is.na(dt$arr_delay))
  n_cancelled_with_dep_delay <- sum(dt$cancelled == 1 & !is.na(dt$dep_delay))
  
  data.frame(
    filename = fname,
    total_records = n_total,
    missing_flight_date = n_missing_date,
    missing_airline = n_missing_airline,
    missing_flight_num = n_missing_flight_num,
    missing_origin = n_missing_origin,
    missing_dest = n_missing_dest,
    missing_crs_dep = n_missing_crs_dep,
    missing_cancelled = n_missing_cancelled,
    invalid_scheduled_time_count = n_invalid_time,
    time_2400_rollover_count = n_rollover_2400,
    cancelled_count = n_cancelled,
    diverted_count = n_diverted,
    valid_arrival_records = n_valid_arr,
    valid_departure_records = n_valid_dep,
    arr_delay_out_of_bounds = n_arr_delay_outside_bounds,
    dep_delay_out_of_bounds = n_dep_delay_outside_bounds,
    cancelled_with_arr_delay = n_cancelled_with_arr_delay,
    cancelled_with_dep_delay = n_cancelled_with_dep_delay,
    stringsAsFactors = FALSE
  )
}

execute_quality_validation_pipeline <- function() {
  csv_files <- sort(list.files(CSV_DIR, pattern = "\\.csv$", full.names = TRUE))
  if (length(csv_files) == 0) {
    log_warn("No CSV files found in {CSV_DIR} to validate.")
    return(invisible(NULL))
  }
  
  reports <- list()
  for (cf in csv_files) {
    reports[[length(reports) + 1]] <- audit_csv_quality(cf)
  }
  
  rep_df <- do.call(rbind, reports)
  write.csv(rep_df, QUALITY_REPORT_FILE, row.names = FALSE)
  log_info("Comprehensive quality report generated at: {QUALITY_REPORT_FILE}")
  print(rep_df)
  invisible(rep_df)
}

if (!interactive()) {
  execute_quality_validation_pipeline()
}
