# ==============================================================================
# tests/testthat/test_benchmarks_reconciliation.R
# Unit and Invariant Verification for Benchmarking & Consistency Reconciliation
# Tests: TEST-09 (MR vs Tez Scaling), TEST-10 (Batch vs Stream Parity),
#        TEST-11 (Fault Tolerance Invariants), TEST-12 (Kafka Latency Monotonicity)
# ==============================================================================

library(testthat)

BASE_DIR <- "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics"
META_DIR <- file.path(BASE_DIR, "dataset", "metadata")

test_that("TEST-09: Hive on Tez consistently delivers >= 4.0x speedup over MapReduce", {
  exec_path <- file.path(META_DIR, "benchmark_execution_time.csv")
  expect_true(file.exists(exec_path))
  
  df_exec <- read.csv(exec_path)
  expect_true(all(df_exec$speedup_factor >= 4.0))
  expect_true(all(df_exec$hive_tez_sec < df_exec$mapreduce_sec))
})

test_that("TEST-10: Batch vs Stream Consistency achieves 0.000% error and 100% key parity", {
  cons_path <- file.path(META_DIR, "benchmark_consistency_reconciliation.csv")
  expect_true(file.exists(cons_path))
  
  df_cons <- read.csv(cons_path)
  expect_true(all(df_cons$record_delta == 0))
  expect_true(all(df_cons$delay_delta == 0.0))
  expect_true(all(df_cons$relative_error_pct == 0.0))
})

test_that("TEST-11: Fault Tolerance deduplication drops exactly 100% of injected duplicate events", {
  ft_path <- file.path(META_DIR, "benchmark_fault_tolerance.csv")
  expect_true(file.exists(ft_path))
  
  df_ft <- read.csv(ft_path)
  # Scenario F1 (5% duplicates): dropped count must equal injected count
  f1 <- df_ft[df_ft$scenario_id == "F1", ]
  expect_equal(f1$injected_events_count, f1$dedup_dropped_count)
  expect_equal(f1$retained_clean_events, 7001618L)
  
  # Scenario F2 (10% duplicates + crash): dropped count must equal injected count
  f2 <- df_ft[df_ft$scenario_id == "F2", ]
  expect_equal(f2$injected_events_count, f2$dedup_dropped_count)
  expect_equal(f2$retained_clean_events, 7001618L)
})

test_that("TEST-12: Kafka latency percentiles exhibit monotonic growth with offered rate", {
  kafka_path <- file.path(META_DIR, "benchmark_kafka_performance.csv")
  expect_true(file.exists(kafka_path))
  
  df_k <- read.csv(kafka_path)
  # p99 >= p95 >= p50 for every rate
  expect_true(all(df_k$p99_latency_ms >= df_k$p95_latency_ms))
  expect_true(all(df_k$p95_latency_ms >= df_k$p50_latency_ms))
  
  # Latency strictly increases as offered rate increases
  expect_true(all(diff(df_k$p50_latency_ms) > 0))
  expect_true(all(diff(df_k$p99_latency_ms) > 0))
})
