# ==============================================================================
# R/generate_subsets.R
# Pipeline Stage 8: Nested Chronological Subsets & Stratified Datasets Generator
# Strict Architecture: R 4.x (data.table, arrow, logger)
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table", repos = "https://cloud.r-project.org")
  if (!requireNamespace("arrow", quietly = TRUE)) install.packages("arrow", repos = "https://cloud.r-project.org")
  if (!requireNamespace("logger", quietly = TRUE)) install.packages("logger", repos = "https://cloud.r-project.org")
  library(data.table)
  library(arrow)
  library(logger)
})

log_file <- file.path("logs", "preprocessing_log.txt")
dir.create("logs", showWarnings = FALSE, recursive = TRUE)
log_appender(appender_tee(log_file))
log_info("Initializing Experimental Subset Generator (Stage 8)")

CLEAN_PARQUET_DIR <- file.path("dataset", "processed", "parquet")
SUBSETS_DIR <- file.path("dataset", "processed", "subsets")
METADATA_DIR <- file.path("dataset", "metadata")
EXPERIMENT_SUMMARY_FILE <- file.path(METADATA_DIR, "experiment_dataset_summary.csv")

dir.create(SUBSETS_DIR, showWarnings = FALSE, recursive = TRUE)

TARGET_SIZES <- c(100000L, 500000L, 1000000L, 5000000L)

export_subset_dual_format <- function(dt_sub, subset_id, sampling_method, seasonality_note) {
  parquet_path <- file.path(SUBSETS_DIR, paste0(subset_id, ".parquet"))
  csv_path <- file.path(SUBSETS_DIR, paste0(subset_id, ".csv"))
  
  arrow::write_parquet(
    dt_sub,
    parquet_path,
    compression = "snappy",
    use_deprecated_int96_timestamps = TRUE
  )
  fwrite(dt_sub, csv_path)
  
  sz_parq <- file.info(parquet_path)$size
  sz_csv  <- file.info(csv_path)$size
  n_rows  <- nrow(dt_sub)
  
  start_ts <- as.character(min(dt_sub$sched_dep_local))
  end_ts   <- as.character(max(dt_sub$sched_dep_local))
  days_span <- round(as.numeric(difftime(max(dt_sub$sched_dep_local), min(dt_sub$sched_dep_local), units = "days")), 2)
  
  log_info("Generated subset '{subset_id}': {n_rows} rows, {days_span} days spanned ({start_ts} to {end_ts})")
  
  data.frame(
    subset_id = subset_id,
    record_count = n_rows,
    parquet_size_bytes = sz_parq,
    csv_size_bytes = sz_csv,
    start_timestamp = start_ts,
    end_timestamp = end_ts,
    calendar_days_spanned = days_span,
    sampling_method = sampling_method,
    seasonality_bias_note = seasonality_note,
    stringsAsFactors = FALSE
  )
}

execute_subset_generation_pipeline <- function() {
  parquet_files <- sort(list.files(CLEAN_PARQUET_DIR, pattern = "On_Time_.*\\.parquet$", full.names = TRUE))
  if (length(parquet_files) == 0) {
    log_warn("No cleaned monthly Parquet files found in {CLEAN_PARQUET_DIR}.")
    return(invisible(NULL))
  }
  
  log_info("Reading {length(parquet_files)} cleaned monthly Parquet files into memory sequentially...")
  monthly_dts <- list()
  for (pf in parquet_files) {
    monthly_dts[[length(monthly_dts) + 1]] <- as.data.table(arrow::read_parquet(pf))
  }
  
  full_dt <- rbindlist(monthly_dts, fill = TRUE)
  rm(monthly_dts)
  gc()
  
  # Ensure strict chronological ordering
  setorder(full_dt, sched_dep_local)
  total_available <- nrow(full_dt)
  log_info("Total cleaned flights available: {total_available}")
  
  summary_records <- list()
  
  # 1. Export Full Dataset
  summary_records[[1]] <- export_subset_dual_format(
    full_dt,
    "flights_full",
    "Complete annual dataset (all valid retained records)",
    "Unbiased full-year empirical coverage"
  )
  
  # 2. Nested Chronological Subsets
  for (target_n in TARGET_SIZES) {
    if (total_available >= target_n) {
      sub_dt <- full_dt[1:target_n]
      subset_name <- sprintf("flights_%s", ifelse(target_n >= 1000000, paste0(target_n / 1000000, "m"), paste0(target_n / 1000, "k")))
      note <- sprintf("Covers first %d flights from beginning of year. High seasonal concentration in early months.", target_n)
      
      summary_records[[length(summary_records) + 1]] <- export_subset_dual_format(
        sub_dt,
        subset_name,
        "Nested chronological slice (first N flights)",
        note
      )
      rm(sub_dt)
      gc()
    } else {
      log_warn("Skipping subset {target_n}: insufficient total records ({total_available})")
    }
  }
  
  # 3. Proportional Monthly Stratified Dataset (1M records)
  target_stratified_n <- 1000000L
  if (total_available >= target_stratified_n) {
    log_info("Generating Proportional Monthly Stratified Dataset (N = {target_stratified_n})...")
    set.seed(42)
    
    # Calculate proportion per month
    full_dt[, month_id := format(sched_dep_local, "%Y-%m")]
    month_counts <- full_dt[, .N, by = month_id]
    month_counts[, prop := N / sum(N)]
    month_counts[, target_k := round(target_stratified_n * prop)]
    
    # Adjust last month for rounding
    discrepancy <- target_stratified_n - sum(month_counts$target_k)
    month_counts[nrow(month_counts), target_k := target_k + discrepancy]
    
    stratified_chunks <- list()
    for (i in seq_len(nrow(month_counts))) {
      m_id <- month_counts$month_id[i]
      k_needed <- month_counts$target_k[i]
      month_rows <- full_dt[month_id == m_id]
      idx <- sample.int(nrow(month_rows), size = k_needed, replace = FALSE)
      stratified_chunks[[i]] <- month_rows[idx]
    }
    
    strat_dt <- rbindlist(stratified_chunks)
    strat_dt[, month_id := NULL]
    setorder(strat_dt, sched_dep_local)
    
    summary_records[[length(summary_records) + 1]] <- export_subset_dual_format(
      strat_dt,
      "flights_1m_proportional",
      "Proportional stratified monthly sampling (seed=42)",
      "Preserves seasonal flight volume variations across all 12 calendar months."
    )
    rm(strat_dt, stratified_chunks)
    gc()
    
    # 4. Equal Monthly Allocation Dataset (1M records - Robustness Experiment)
    log_info("Generating Equal Monthly Allocation Dataset (N = {target_stratified_n})...")
    k_per_month <- floor(target_stratified_n / nrow(month_counts))
    equal_chunks <- list()
    for (i in seq_len(nrow(month_counts))) {
      m_id <- month_counts$month_id[i]
      month_rows <- full_dt[month_id == m_id]
      idx <- sample.int(nrow(month_rows), size = min(k_per_month, nrow(month_rows)), replace = FALSE)
      equal_chunks[[i]] <- month_rows[idx]
    }
    equal_dt <- rbindlist(equal_chunks)
    equal_dt[, month_id := NULL]
    setorder(equal_dt, sched_dep_local)
    
    summary_records[[length(summary_records) + 1]] <- export_subset_dual_format(
      equal_dt,
      "flights_1m_equal",
      "Equal monthly allocation sampling (N/12 per month, seed=42)",
      "Artificially balances month representation; removes volume weighting to isolate seasonal volatility."
    )
    rm(equal_dt, equal_chunks)
    gc()
    
    full_dt[, month_id := NULL]
  }
  
  exp_summary_df <- do.call(rbind, summary_records)
  write.csv(exp_summary_df, EXPERIMENT_SUMMARY_FILE, row.names = FALSE)
  log_info("Experiment dataset summary written to: {EXPERIMENT_SUMMARY_FILE}")
  print(exp_summary_df[, c("subset_id", "record_count", "calendar_days_spanned", "sampling_method")])
  invisible(exp_summary_df)
}

if (!interactive()) {
  execute_subset_generation_pipeline()
}
