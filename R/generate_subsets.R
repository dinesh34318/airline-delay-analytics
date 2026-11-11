# ==============================================================================
# R/generate_subsets.R
# Pipeline Stage 8: Nested Chronological Subsets & Stratified Datasets Generator
# Technology: R 4.x (arrow, dplyr)
# ==============================================================================

.libPaths(c("C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
})

BASE_DIR        <- "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics"
PARQUET_DIR     <- file.path(BASE_DIR, "dataset", "processed", "parquet")
SUBSETS_DIR     <- file.path(BASE_DIR, "dataset", "processed", "subsets")
METADATA_DIR    <- file.path(BASE_DIR, "dataset", "metadata")
SUMMARY_FILE    <- file.path(METADATA_DIR, "experiment_dataset_summary.csv")
LOG_FILE        <- file.path(BASE_DIR, "logs", "subset_generation.log")

dir.create(SUBSETS_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(METADATA_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(BASE_DIR, "logs"), recursive = TRUE, showWarnings = FALSE)

log_msg <- function(msg) {
  ts   <- format(Sys.time(), "%Y-%m-%d %H:%M:%S UTC", tz = "UTC")
  line <- paste0("[", ts, "] ", msg)
  message(line)
  write(line, file = LOG_FILE, append = TRUE)
}

log_msg("====================================================================")
log_msg("STAGE 8 - Experimental Subset Generation (Arrow + dplyr)")
log_msg("====================================================================")

# Function to export subset in dual format (Parquet & CSV)
export_subset <- function(df, subset_id, sampling_method, seasonality_note) {
  parquet_path <- file.path(SUBSETS_DIR, paste0(subset_id, ".parquet"))
  csv_path     <- file.path(SUBSETS_DIR, paste0(subset_id, ".csv"))
  
  log_msg(paste0("Writing subset '", subset_id, "' (", nrow(df), " rows)..."))
  
  arrow::write_parquet(df, parquet_path, compression = "snappy")
  arrow::write_csv_arrow(df, csv_path)
  
  sz_parq   <- file.info(parquet_path)$size
  sz_csv    <- file.info(csv_path)$size
  n_rows    <- nrow(df)
  start_dt  <- as.character(min(df$flight_date, na.rm = TRUE))
  end_dt    <- as.character(max(df$flight_date, na.rm = TRUE))
  days_span <- as.numeric(difftime(as.Date(end_dt), as.Date(start_dt), units = "days")) + 1
  
  log_msg(paste0("Subset '", subset_id, "' generated: ", n_rows, " rows (", 
                 round(sz_parq / 1024 / 1024, 2), " MB Parquet, ", 
                 round(sz_csv / 1024 / 1024, 2), " MB CSV), Span: ", 
                 start_dt, " to ", end_dt, " (", days_span, " days)"))
  
  data.frame(
    subset_id             = subset_id,
    record_count          = n_rows,
    parquet_size_bytes    = sz_parq,
    csv_size_bytes        = sz_csv,
    start_date            = start_dt,
    end_date              = end_dt,
    calendar_days_spanned = days_span,
    sampling_method       = sampling_method,
    seasonality_bias_note = seasonality_note,
    stringsAsFactors      = FALSE
  )
}

# Open all 12 parquet files as a unified Arrow dataset
ds <- arrow::open_dataset(PARQUET_DIR)
total_flights <- ds %>% count() %>% collect() %>% pull(n)
log_msg(paste("Total flights in dataset:", format(total_flights, big.mark = ",")))

summary_list <- list()

# -----------------------------------------------------------------------------
# 1. Chronological subsets: 100k, 500k, 1m, 5m
# Because data is organized across months 01 through 12, files 01..12 are already
# month-ordered. We can extract target sizes by pulling from early months.
# -----------------------------------------------------------------------------
files <- sort(list.files(PARQUET_DIR, pattern = "^clean_2025_.*\\.parquet$", full.names = TRUE))
log_msg(paste("Found", length(files), "monthly parquet files"))

# Read month 1 for 100k
log_msg("Loading initial files for chronological subsets...")
df_m1 <- arrow::read_parquet(files[1]) %>% arrange(flight_date, crs_dep)

# 100K subset
sub_100k <- head(df_m1, 100000)
summary_list[[length(summary_list) + 1]] <- export_subset(
  sub_100k, "flights_100k",
  "Nested chronological slice (first 100K flights)",
  "Covers first ~5 days of January 2025. Severe early winter seasonality."
)
rm(sub_100k); gc()

# 500K subset
sub_500k <- head(df_m1, 500000)
summary_list[[length(summary_list) + 1]] <- export_subset(
  sub_500k, "flights_500k",
  "Nested chronological slice (first 500K flights)",
  "Covers majority of January 2025 (~28 days). January post-holiday seasonality."
)
rm(sub_500k); gc()

# 1M subset: combine month 1 and month 2
df_m2 <- arrow::read_parquet(files[2]) %>% arrange(flight_date, crs_dep)
df_m1_m2 <- bind_rows(df_m1, df_m2) %>% arrange(flight_date, crs_dep)
rm(df_m1, df_m2); gc()

sub_1m <- head(df_m1_m2, 1000000)
summary_list[[length(summary_list) + 1]] <- export_subset(
  sub_1m, "flights_1m",
  "Nested chronological slice (first 1M flights)",
  "Spans January to late February 2025. Winter flight distribution."
)
rm(sub_1m, df_m1_m2); gc()

# -----------------------------------------------------------------------------
# 2. Stratified Sampling across all 12 months (1M Proportional & 1M Equal)
# Process month by month to avoid 16 GB RAM exhaustion
# -----------------------------------------------------------------------------
log_msg("Generating stratified 1M subsets across all 12 months...")

# Get monthly counts from statistical_profile.csv or compute from files
monthly_meta_path <- file.path(METADATA_DIR, "statistical_profile.csv")
if (file.exists(monthly_meta_path)) {
  meta_df <- read.csv(monthly_meta_path)
  m_counts <- meta_df$total_flights
} else {
  m_counts <- sapply(files, function(f) arrow::open_dataset(f) %>% count() %>% collect() %>% pull(n))
}

prop_k <- round(1000000 * (m_counts / sum(m_counts)))
# Adjust last month for exact 1,000,000
prop_k[length(prop_k)] <- prop_k[length(prop_k)] + (1000000 - sum(prop_k))

equal_k <- rep(floor(1000000 / 12), 12)
equal_k[12] <- equal_k[12] + (1000000 - sum(equal_k))

set.seed(42)
prop_samples  <- list()
equal_samples <- list()

for (i in seq_along(files)) {
  log_msg(paste0("Sampling from month ", i, " (file: ", basename(files[i]), "): prop=", prop_k[i], ", equal=", equal_k[i]))
  m_df <- arrow::read_parquet(files[i])
  n_total <- nrow(m_df)
  
  # Sample proportional
  idx_p <- sample.int(n_total, size = min(prop_k[i], n_total), replace = FALSE)
  prop_samples[[i]] <- m_df[idx_p, ]
  
  # Sample equal
  idx_e <- sample.int(n_total, size = min(equal_k[i], n_total), replace = FALSE)
  equal_samples[[i]] <- m_df[idx_e, ]
  
  rm(m_df); gc()
}

df_prop <- bind_rows(prop_samples) %>% arrange(flight_date, crs_dep)
rm(prop_samples); gc()
summary_list[[length(summary_list) + 1]] <- export_subset(
  df_prop, "flights_1m_proportional",
  "Proportional monthly stratified random sample (seed=42)",
  "Preserves empirical seasonal flight distribution across all 12 calendar months."
)
rm(df_prop); gc()

df_equal <- bind_rows(equal_samples) %>% arrange(flight_date, crs_dep)
rm(equal_samples); gc()
summary_list[[length(summary_list) + 1]] <- export_subset(
  df_equal, "flights_1m_equal",
  "Equal monthly allocation random sample (~83,333 flights/month, seed=42)",
  "Artificially balances month representation; removes volume weighting to isolate seasonal volatility."
)
rm(df_equal); gc()

# Combine summary and write
summary_df <- do.call(rbind, summary_list)
write.csv(summary_df, SUMMARY_FILE, row.names = FALSE)
log_msg(paste("Experiment dataset summary written to:", SUMMARY_FILE))
print(summary_df[, c("subset_id", "record_count", "start_date", "end_date", "calendar_days_spanned")])

log_msg("STAGE 8 - Experimental Subset Generation Completed Successfully.")
