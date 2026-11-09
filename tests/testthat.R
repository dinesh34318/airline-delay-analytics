# ==============================================================================
# tests/testthat.R
# Test Runner for Airline Delay Analytics Pipeline
# ==============================================================================

if (!requireNamespace("testthat", quietly = TRUE)) {
  install.packages("testthat", repos = "https://cloud.r-project.org")
}
library(testthat)

test_check("AirlineDelayAnalytics", test_paths = file.path("tests", "testthat"))
