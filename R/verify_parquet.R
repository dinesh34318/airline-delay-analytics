# ==============================================================================
# R/verify_parquet.R
# Pipeline Stage 9: Parquet/CSV Parity Verification & Hive Schema Conformance
# Strict Architecture: R 4.x (arrow, data.table, logger)
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("arrow", quietly = TRUE)) install.packages("arrow", repos = "https://cloud.r-project.org")
  if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table", repos = "https://cloud.r-project.org")
  if (!requireNamespace("logger", quietly = TRUE)) install.packages("logger", repos = "https://cloud.r-project.org")
  library(arrow)
  library(data.table)
  library(logger)
})

log_file <- file.path("logs", "preprocessing_log.txt")
dir.create("logs", showWarnings = FALSE, recursive = TRUE)
log_appender(appender_tee(log_file))
log_info("Initializing Parquet & CSV Verification Engine (Stage 9)")

SUBSETS_DIR <- file.path("dataset", "processed", "subsets")
METADATA_DIR <- file.path("dataset", "metadata")
VERIFICATION_LOG <- file.path("logs", "verification_report.txt")

verify_single_subset_parity <- function(subset_base_name) {
  parq_path <- file.path(SUBSETS_DIR, paste0(subset_base_name, ".parquet"))
  csv_path  <- file.path(SUBSETS_DIR, paste0(subset_base_name, ".csv"))
  
  if (!file.exists(parq_path) || !file.exists(csv_path)) {
    log_warn("Subset files for {subset_base_name} not found. Skipping.")
    return(NULL)
  }
  
  log_info("Verifying Parquet vs CSV parity for: {subset_base_name}")
  
  dt_parq <- as.data.table(arrow::read_parquet(parq_path))
  dt_csv  <- fread(csv_path)
  
  # 1. Row count match
  n_parq <- nrow(dt_parq)
  n_csv  <- nrow(dt_csv)
  if (n_parq != n_csv) {
    stop(sprintf("PARITY VIOLATION in %s: Parquet rows (%d) != CSV rows (%d)", subset_base_name, n_parq, n_csv))
  }
  
  # 2. Additive measures parity (W1-W4 components)
  sum_arr_parq <- sum(fifelse(dt_parq$is_valid_arr == 1L, dt_parq$arr_delay, 0), na.rm = TRUE)
  sum_arr_csv  <- sum(fifelse(dt_csv$is_valid_arr == 1L, dt_csv$arr_delay, 0), na.rm = TRUE)
  diff_arr <- abs(sum_arr_parq - sum_arr_csv)
  
  sum_dep_parq <- sum(fifelse(dt_parq$is_valid_dep == 1L, dt_parq$dep_delay, 0), na.rm = TRUE)
  sum_dep_csv  <- sum(fifelse(dt_csv$is_valid_dep == 1L, dt_csv$dep_delay, 0), na.rm = TRUE)
  diff_dep <- abs(sum_dep_parq - sum_dep_csv)
  
  n_canc_parq <- sum(dt_parq$cancelled)
  n_canc_csv  <- sum(dt_csv$cancelled)
  
  n_valid_arr_parq <- sum(dt_parq$is_valid_arr)
  n_valid_arr_csv  <- sum(dt_csv$is_valid_arr)
  
  n_valid_dep_parq <- sum(dt_parq$is_valid_dep)
  n_valid_dep_csv  <- sum(dt_csv$is_valid_dep)
  
  # Assertions
  if (diff_arr > 1e-4) {
    stop(sprintf("AGGREGATE DIVERGENCE in %s: Arrival delay sum differs by %f", subset_base_name, diff_arr))
  }
  if (diff_dep > 1e-4) {
    stop(sprintf("AGGREGATE DIVERGENCE in %s: Departure delay sum differs by %f", subset_base_name, diff_dep))
  }
  if (n_canc_parq != n_canc_csv) {
    stop(sprintf("CANCELLATION DIVERGENCE in %s: Parquet (%d) != CSV (%d)", subset_base_name, n_canc_parq, n_canc_csv))
  }
  if (n_valid_arr_parq != n_valid_arr_csv) {
    stop(sprintf("VALID ARRIVAL DIVERGENCE in %s: Parquet (%d) != CSV (%d)", subset_base_name, n_valid_arr_parq, n_valid_arr_csv))
  }
  if (n_valid_dep_parq != n_valid_dep_csv) {
    stop(sprintf("VALID DEPARTURE DIVERGENCE in %s: Parquet (%d) != CSV (%d)", subset_base_name, n_valid_dep_parq, n_valid_dep_csv))
  }
  
  log_info("PARITY CONFIRMED for {subset_base_name}: {n_parq} rows, arr_delay_sum = {round(sum_arr_parq, 2)}, canc = {n_canc_parq}")
  
  data.frame(
    subset_id = subset_base_name,
    row_count = n_parq,
    sum_arrival_delay = round(sum_arr_parq, 2),
    sum_departure_delay = round(sum_dep_parq, 2),
    cancelled_count = n_canc_parq,
    valid_arrivals = n_valid_arr_parq,
    valid_departures = n_valid_dep_parq,
    parity_status = "PASSED",
    stringsAsFactors = FALSE
  )
}

execute_verification_pipeline <- function() {
  parq_files <- list.files(SUBSETS_DIR, pattern = "\\.parquet$")
  base_names <- sub("\\.parquet$", "", parq_files)
  
  if (length(base_names) == 0) {
    log_warn("No subsets found in {SUBSETS_DIR} to verify.")
    return(invisible(NULL))
  }
  
  results <- list()
  for (bn in base_names) {
    res <- verify_single_subset_parity(bn)
    if (!is.null(res)) results[[length(results) + 1]] <- res
  }
  
  res_df <- do.call(rbind, results)
  write.csv(res_df, file.path(METADATA_DIR, "parquet_csv_parity_report.csv"), row.names = FALSE)
  log_info("Parity report saved to dataset/metadata/parquet_csv_parity_report.csv")
  print(res_df)
  invisible(res_df)
}

if (!interactive()) {
  execute_verification_pipeline()
}
