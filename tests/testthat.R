# ==============================================================================
# tests/testthat.R
# Test Runner for Airline Delay Analytics Pipeline
# ==============================================================================

.libPaths(c("C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(testthat)
})

test_results <- testthat::test_dir(
  "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/tests/testthat",
  stop_on_failure = TRUE
)

print(test_results)
