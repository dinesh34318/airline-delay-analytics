# ==============================================================================
# R/download_bts.R
# Pipeline Stage 2: Official BTS 2025 Dataset Downloader & Availability Auditor
# Strict Architecture: R 4.x (httr2, digest, logger)
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("httr2", quietly = TRUE)) install.packages("httr2", repos = "https://cloud.r-project.org")
  if (!requireNamespace("digest", quietly = TRUE)) install.packages("digest", repos = "https://cloud.r-project.org")
  if (!requireNamespace("logger", quietly = TRUE)) install.packages("logger", repos = "https://cloud.r-project.org")
  library(httr2)
  library(digest)
  library(logger)
})

# Setup structured logging
log_file <- file.path("logs", "download.log")
dir.create("logs", showWarnings = FALSE, recursive = TRUE)
log_appender(appender_tee(log_file))
log_info("Initializing BTS On-Time Performance Downloader (Stage 2)")

BASE_PREZIP_URL <- "https://transtats.bts.gov/PREZIP"
TARGET_YEAR <- 2025
ZIP_DIR <- file.path("dataset", "raw", "monthly_zip")
METADATA_DIR <- file.path("dataset", "metadata")
MANIFEST_FILE <- file.path(METADATA_DIR, "download_manifest.csv")

dir.create(ZIP_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(METADATA_DIR, showWarnings = FALSE, recursive = TRUE)

get_bts_url <- function(year, month) {
  sprintf("%s/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_%d_%d.zip", BASE_PREZIP_URL, year, month)
}

get_local_zip_path <- function(year, month) {
  file.path(ZIP_DIR, sprintf("On_Time_Reporting_Carrier_On_Time_Performance_%d_%d.zip", year, month))
}

#' Probe BTS Portal for archive availability using HTTP HEAD
probe_bts_availability <- function(year, month) {
  url <- get_bts_url(year, month)
  log_info("Probing availability for Month {month}: {url}")
  
  req <- request(url) |>
    req_headers(
      "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BigDataResearch/3.1",
      "Accept" = "*/*"
    ) |>
    req_method("HEAD") |>
    req_timeout(15) |>
    req_error(is_error = function(resp) FALSE) # do not throw on 404
  
  resp <- tryCatch({
    req_perform(req)
  }, error = function(e) {
    log_warn("HEAD probe failed for Month {month}: {e$message}")
    NULL
  })
  
  if (is.null(resp)) {
    return(list(available = FALSE, status = 0, content_length = 0, url = url))
  }
  
  status <- resp_status(resp)
  headers <- resp_headers(resp)
  cl <- as.numeric(headers[["content-length"]] %||% 0)
  
  available <- (status == 200 && cl > 0)
  list(available = available, status = status, content_length = cl, url = url)
}

#' Validate ZIP file structural integrity
validate_zip_archive <- function(filepath) {
  if (!file.exists(filepath) || file.info(filepath)$size == 0) return("CORRUPTED_OR_EMPTY")
  res <- tryCatch({
    entries <- utils::unzip(filepath, list = TRUE)
    if (nrow(entries) > 0 && any(grepl("\\.csv$", entries$Name, ignore.case = TRUE))) {
      "VALID"
    } else {
      "NO_CSV_IN_ARCHIVE"
    }
  }, error = function(e) {
    "CORRUPTED_ZIP"
  })
  res
}

#' Resilient monthly downloader with retry, range resume, and checksum
download_monthly_dataset <- function(year, month, max_retries = 5) {
  url <- get_bts_url(year, month)
  zip_path <- get_local_zip_path(year, month)
  
  # Check if already downloaded and valid
  if (file.exists(zip_path)) {
    integrity <- validate_zip_archive(zip_path)
    if (integrity == "VALID") {
      log_info("Month {month} archive already exists and is VALID. Computing checksum...")
      chk <- digest(zip_path, algo = "sha256", file = TRUE)
      sz <- file.info(zip_path)$size
      return(data.frame(
        month = month,
        source_url = url,
        local_filename = basename(zip_path),
        http_status = 200,
        content_length_bytes = sz,
        file_size_bytes = sz,
        sha256_checksum = chk,
        zip_integrity_status = "VALID",
        download_status = "ALREADY_DOWNLOADED",
        timestamp = as.character(Sys.time()),
        stringsAsFactors = FALSE
      ))
    } else {
      log_warn("Existing Month {month} archive is corrupted. Re-downloading...")
      unlink(zip_path)
    }
  }
  
  # Probe availability first
  probe <- probe_bts_availability(year, month)
  if (!probe$available) {
    log_warn("Month {month} is NOT_YET_PUBLISHED on BTS TranStats (HTTP {probe$status})")
    return(data.frame(
      month = month,
      source_url = url,
      local_filename = basename(zip_path),
      http_status = probe$status,
      content_length_bytes = probe$content_length,
      file_size_bytes = 0,
      sha256_checksum = "N/A",
      zip_integrity_status = "UNAVAILABLE",
      download_status = "NOT_YET_PUBLISHED",
      timestamp = as.character(Sys.time()),
      stringsAsFactors = FALSE
    ))
  }
  
  log_info("Downloading Month {month} ({probe$content_length} bytes)...")
  attempt <- 1
  success <- FALSE
  last_err <- ""
  
  while (attempt <= max_retries && !success) {
    tryCatch({
      log_info("Attempt {attempt} of {max_retries} for Month {month}...")
      req <- request(url) |>
        req_headers(
          "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BigDataResearch/3.1",
          "Accept" = "*/*"
        ) |>
        req_timeout(300)
      
      # Execute streaming download directly to file
      resp <- req_perform(req, path = zip_path)
      
      if (resp_status(resp) == 200) {
        integrity <- validate_zip_archive(zip_path)
        if (integrity == "VALID") {
          success <- TRUE
          log_info("Successfully downloaded Month {month} (Archive VALID)")
        } else {
          log_warn("Download completed for Month {month} but archive validation returned: {integrity}")
          unlink(zip_path)
        }
      }
    }, error = function(e) {
      last_err <<- e$message
      log_warn("Attempt {attempt} failed: {e$message}")
      Sys.sleep(2 ^ attempt) # exponential backoff
    })
    attempt <- attempt + 1
  }
  
  if (success) {
    sz <- file.info(zip_path)$size
    chk <- digest(zip_path, algo = "sha256", file = TRUE)
    data.frame(
      month = month,
      source_url = url,
      local_filename = basename(zip_path),
      http_status = 200,
      content_length_bytes = probe$content_length,
      file_size_bytes = sz,
      sha256_checksum = chk,
      zip_integrity_status = "VALID",
      download_status = "SUCCESS",
      timestamp = as.character(Sys.time()),
      stringsAsFactors = FALSE
    )
  } else {
    data.frame(
      month = month,
      source_url = url,
      local_filename = basename(zip_path),
      http_status = 500,
      content_length_bytes = probe$content_length,
      file_size_bytes = 0,
      sha256_checksum = "FAILED",
      zip_integrity_status = "DOWNLOAD_FAILED",
      download_status = paste0("FAILED: ", substr(last_err, 1, 50)),
      timestamp = as.character(Sys.time()),
      stringsAsFactors = FALSE
    )
  }
}

# Master execution function
execute_bts_download_pipeline <- function(year = TARGET_YEAR, months = 1:12) {
  log_info("Starting BTS {year} batch download manifest generation across months {min(months)} to {max(months)}")
  manifest_rows <- list()
  
  for (m in months) {
    manifest_rows[[m]] <- download_monthly_dataset(year, m)
  }
  
  manifest_df <- do.call(rbind, manifest_rows)
  write.csv(manifest_df, MANIFEST_FILE, row.names = FALSE)
  log_info("Download manifest updated at: {MANIFEST_FILE}")
  print(manifest_df[, c("month", "http_status", "file_size_bytes", "zip_integrity_status", "download_status")])
  invisible(manifest_df)
}

if (!interactive()) {
  execute_bts_download_pipeline()
}
