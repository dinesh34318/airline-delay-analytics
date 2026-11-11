# ==============================================================================
# run_dashboard.R
# Convenience launcher for the Stage 11 Interactive Shiny Dashboard
# ==============================================================================

# Point R to the local user library where shiny, ggplot2, and arrow are installed
project_lib <- "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/R/library"
.libPaths(c(project_lib, .libPaths()))

suppressPackageStartupMessages({
  library(shiny)
})

app_dir <- "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/shiny"

cat("====================================================================\n")
cat("Starting Airline Delay Analytics Shiny Dashboard...\n")
cat("Listening on: http://127.0.0.1:8080\n")
cat("Press Ctrl+C to stop the dashboard server.\n")
cat("====================================================================\n")

shiny::runApp(app_dir, port = 8080, host = "127.0.0.1", launch.browser = TRUE)
