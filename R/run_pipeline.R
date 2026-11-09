# ==============================================================================
# R/run_pipeline.R
# Master End-to-End Orchestrator: BTS 2025 Dataset Engineering Pipeline
# Strict Architecture: R 4.x (No Python, No ML frameworks)
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("logger", quietly = TRUE)) install.packages("logger", repos = "https://cloud.r-project.org")
  library(logger)
})

log_file <- file.path("logs", "preprocessing_log.txt")
dir.create("logs", showWarnings = FALSE, recursive = TRUE)
log_appender(appender_tee(log_file))

run_pipeline_stage <- function(stage_num, stage_name, script_path) {
  banner <- sprintf("========================================================\n STAGE %d: %s\n Script: %s\n========================================================", stage_num, stage_name, script_path)
  cat("\n", banner, "\n\n")
  log_info("Starting Stage {stage_num}: {stage_name}")
  
  t0 <- Sys.time()
  tryCatch({
    source(script_path, local = new.env())
    t1 <- Sys.time()
    elapsed <- round(as.numeric(difftime(t1, t0, units = "secs")), 2)
    log_info("Stage {stage_num} [{stage_name}] COMPLETED successfully in {elapsed} seconds.")
    TRUE
  }, error = function(e) {
    log_error("Stage {stage_num} [{stage_name}] FAILED: {e$message}")
    stop(e)
  })
}

run_full_data_engineering_pipeline <- function(skip_download = FALSE) {
  cat("\n################################################################################\n")
  cat(" AIRLINE DELAY ANALYTICS: DATASET COLLECTION & PREPROCESSING PIPELINE (v3.1)\n")
  cat(" Technology Stack: R 4.x Data Engineering (Parquet + CSV for HDP 2.5 Sandbox)\n")
  cat("################################################################################\n\n")
  
  total_t0 <- Sys.time()
  
  # Stage 2: Download & Availability Probe
  if (!skip_download) {
    run_pipeline_stage(2, "BTS 2025 Portal Probing & Download", "R/download_bts.R")
  } else {
    log_info("Skipping Stage 2 (Download) as requested.")
  }
  
  # Stage 3: Safe Extraction & Zip Slip Protection
  run_pipeline_stage(3, "Safe Archive Extraction", "R/extract_archives.R")
  
  # Stage 4: Dataset Profiling & Dictionary Generation
  run_pipeline_stage(4, "Dataset Profiling & Dictionary Generation", "R/inspect_dataset.R")
  
  # Stage 5: Quality Audit & Anomaly Detection
  run_pipeline_stage(5, "Data Quality & Anomaly Audit", "R/validate_data.R")
  
  # Stage 6: Chunked Preprocessing, Flags & Quarantine
  run_pipeline_stage(6, "Chunked Cleansing, Flags & Quarantine", "R/preprocess.R")
  
  # Stage 8: Experimental Subsets & Stratified Datasets
  run_pipeline_stage(8, "Experimental Subsets & Stratified Sampling", "R/generate_subsets.R")
  
  # Stage 9: Parquet & CSV Parity Verification
  run_pipeline_stage(9, "Parquet & CSV Parity Verification", "R/verify_parquet.R")
  
  total_t1 <- Sys.time()
  total_elapsed <- round(as.numeric(difftime(total_t1, total_t0, units = "mins")), 2)
  cat("\n################################################################################\n")
  cat(sprintf(" PIPELINE EXECUTION COMPLETED SUCCESSFULLY IN %s MINUTES\n", total_elapsed))
  cat(" Datasets ready in: dataset/processed/parquet/ and dataset/processed/csv/\n")
  cat(" Subsets ready in:  dataset/processed/subsets/\n")
  cat(" Metadata ready in: dataset/metadata/\n")
  cat("################################################################################\n\n")
}

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  skip_dl <- "--skip-download" %in% args
  run_full_data_engineering_pipeline(skip_download = skip_dl)
}
