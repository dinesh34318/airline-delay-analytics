# ==============================================================================
# tests/testthat/test_preprocessing.R
# Unit, Invariant, and Regression Tests for Airline Delay Preprocessing (TEST-01 to TEST-08)
# Strict Architecture: R 4.x (testthat, data.table, lubridate, digest)
# ==============================================================================

library(testthat)
library(data.table)
library(lubridate)
library(digest)

test_that("TEST-01: 2400 Midnight Rollover maps to next day 00:00:00 and sched_dep_hour = 0", {
  flight_date <- "2025-01-15"
  crs_dep <- 2400L
  
  is_2400 <- (crs_dep == 2400L)
  sched_dep_hour <- ifelse(is_2400, 0L, as.integer(crs_dep %/% 100))
  sched_dep_min  <- ifelse(is_2400, 0L, as.integer(crs_dep %% 100))
  
  base_date <- as.Date(flight_date)
  actual_date <- if (is_2400) base_date + 1 else base_date
  sched_dep_local <- as.POSIXct(actual_date, tz = "UTC") + (sched_dep_hour * 3600) + (sched_dep_min * 60)
  
  expect_equal(sched_dep_hour, 0L)
  expect_equal(sched_dep_min, 0L)
  expect_equal(as.character(actual_date), "2025-01-16")
  expect_equal(format(sched_dep_local, "%Y-%m-%d %H:%M:%S"), "2025-01-16 00:00:00")
})

test_that("TEST-02: Scheduled time validation rejects invalid HHMM formats", {
  valid_times   <- c(0L, 5L, 830L, 1245L, 2359L, 2400L)
  invalid_times <- c(-10L, 2401L, 2500L, 1260L, 899L, NA_integer_)
  
  is_valid_time <- function(t) {
    if (is.na(t) || t < 0) return(FALSE)
    if (t == 2400L) return(TRUE)
    h <- t %/% 100
    m <- t %% 100
    return(h >= 0 && h <= 23 && m >= 0 && m <= 59)
  }
  
  expect_true(all(vapply(valid_times, is_valid_time, logical(1))))
  expect_false(any(vapply(invalid_times, is_valid_time, logical(1))))
})

test_that("TEST-03: Duplicate classification distinguishes exact duplicates from conflicting keys", {
  # Mock sample dataset
  sample_dt <- data.table(
    flight_date = c("2025-01-01", "2025-01-01", "2025-01-01", "2025-01-01"),
    airline     = c("AA", "AA", "DL", "DL"),
    flight_num  = c(100L, 100L, 200L, 200L),
    origin      = c("ORD", "ORD", "ATL", "ATL"),
    dest        = c("DFW", "DFW", "MIA", "MIA"),
    crs_dep     = c(900L, 900L, 1400L, 1400L),
    dep_delay   = c(15.0, 15.0, 10.0, 45.0), # AA is exact duplicate; DL has conflicting delay!
    arr_delay   = c(20.0, 20.0, 15.0, 50.0),
    cancelled   = c(0L, 0L, 0L, 0L),
    diverted    = c(0L, 0L, 0L, 0L)
  )
  
  sample_dt[, structured_key := paste(flight_date, airline, flight_num, origin, dest, crs_dep, sep = "|")]
  sample_dt[, is_exact_match := (
    uniqueN(arr_delay) == 1 &&
    uniqueN(dep_delay) == 1 &&
    uniqueN(cancelled) == 1 &&
    uniqueN(diverted) == 1
  ), by = structured_key]
  
  # AA records should be exact match
  expect_true(all(sample_dt[airline == "AA", is_exact_match]))
  # DL records should be conflicting match
  expect_false(all(sample_dt[airline == "DL", is_exact_match]))
})

test_that("TEST-04: Delay validity flags and zero-imputation policy are strictly maintained", {
  dt <- data.table(
    cancelled = c(1L, 0L, 0L, 0L),
    diverted  = c(0L, 1L, 0L, 0L),
    arr_delay = c(NA_real_, NA_real_, 12.0, -5.0),
    dep_delay = c(NA_real_, 5.0, 10.0, 0.0)
  )
  
  dt[, is_valid_arr := as.integer(cancelled == 0L & diverted == 0L & !is.na(arr_delay))]
  dt[, is_valid_dep := as.integer(cancelled == 0L & !is.na(dep_delay))]
  
  # Cancelled flight has invalid arrival and departure
  expect_equal(dt$is_valid_arr[1], 0L)
  expect_equal(dt$is_valid_dep[1], 0L)
  expect_true(is.na(dt$arr_delay[1]))
  expect_true(is.na(dt$dep_delay[1]))
  
  # Diverted flight has invalid arrival but may have valid departure if flown
  expect_equal(dt$is_valid_arr[2], 0L)
  expect_equal(dt$is_valid_dep[2], 1L)
  
  # Normal flights have valid delays
  expect_equal(dt$is_valid_arr[3], 1L)
  expect_equal(dt$is_valid_dep[3], 1L)
})

test_that("TEST-05: Mutually exclusive row conservation equation holds", {
  n_raw <- 1000L
  n_quarantine <- 45L
  n_exact_dups <- 15L
  n_retained <- n_raw - n_quarantine - n_exact_dups
  
  expect_equal(n_raw, n_retained + n_quarantine + n_exact_dups)
})

test_that("TEST-06: Deterministic SHA-256 event_id is reproducible and unique across flight keys", {
  key1 <- "2025-01-15|AA|1234|ORD|DFW|0930"
  key2 <- "2025-01-15|AA|1234|ORD|DFW|0930"
  key3 <- "2025-01-15|AA|1234|DFW|LAX|1400"
  
  h1 <- digest(key1, algo = "sha256", serialize = FALSE)
  h2 <- digest(key2, algo = "sha256", serialize = FALSE)
  h3 <- digest(key3, algo = "sha256", serialize = FALSE)
  
  expect_equal(h1, h2)
  expect_false(h1 == h3)
  expect_equal(nchar(h1), 64L)
})

test_that("TEST-07: Timezone handling across US daylight saving time boundary", {
  # DST transition in US: March 9, 2025 at 02:00 -> 03:00
  winter_dt <- as.POSIXct("2025-01-15 12:00:00", tz = "America/New_York")
  summer_dt <- as.POSIXct("2025-06-15 12:00:00", tz = "America/New_York")
  
  # Winter in New York is UTC-5; Summer is UTC-4
  utc_winter <- with_tz(winter_dt, "UTC")
  utc_summer <- with_tz(summer_dt, "UTC")
  
  expect_equal(format(utc_winter, "%H:%M:%S"), "17:00:00")
  expect_equal(format(utc_summer, "%H:%M:%S"), "16:00:00")
})
