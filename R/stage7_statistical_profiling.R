# ==============================================================================
# R/stage7_statistical_profiling.R
# Stage 7: Statistical Profiling & Visualisation of Cleaned Parquet Data
# Technology: R 4.x | Packages: arrow, dplyr, ggplot2, lubridate, scales
# ==============================================================================

# Add user library path where packages were installed
.libPaths(c("C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/R/library", .libPaths()))
# Input  : dataset/processed/parquet/clean_2025_MM.parquet (12 files)
# Outputs: dataset/metadata/statistical_profile.csv
#          dataset/metadata/carrier_profile.csv
#          dataset/metadata/route_profile.csv
#          R/plots/  (PNG charts)
#          logs/r_profiling.log
# ==============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(lubridate)
  library(scales)
})

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR    <- "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics"
PARQUET_DIR <- file.path(BASE_DIR, "dataset", "processed", "parquet")
META_DIR    <- file.path(BASE_DIR, "dataset", "metadata")
PLOT_DIR    <- file.path(BASE_DIR, "R", "plots")
LOG_FILE    <- file.path(BASE_DIR, "logs", "r_profiling.log")

dir.create(PLOT_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(META_DIR, recursive = TRUE, showWarnings = FALSE)

# ── Logger ────────────────────────────────────────────────────────────────────
log_msg <- function(msg) {
  ts   <- format(Sys.time(), "%Y-%m-%d %H:%M:%S UTC", tz = "UTC")
  line <- paste0("[", ts, "] ", msg)
  message(line)
  write(line, file = LOG_FILE, append = TRUE)
}

# ── Load all parquet files into one Arrow dataset ─────────────────────────────
log_msg("====================================================================")
log_msg("STAGE 7 - Statistical Profiling (R 4.x + Arrow)")
log_msg("====================================================================")

parquet_files <- list.files(PARQUET_DIR, pattern = "\\.parquet$", full.names = TRUE)
log_msg(paste("Found", length(parquet_files), "parquet files"))

if (length(parquet_files) == 0) {
  stop("No parquet files found in ", PARQUET_DIR, ". Run Stage 6 first.")
}

# Read all into one dataframe (Arrow lazy evaluation)
ds <- open_dataset(PARQUET_DIR, format = "parquet")
log_msg(paste("Dataset schema loaded. Computing row count..."))
total_rows <- ds %>% count() %>% collect() %>% pull(n)
log_msg(paste("Total clean rows:", format(total_rows, big.mark = ",")))

# Collect full dataset for profiling (chunked by month via arrow)
df <- ds %>% collect()
log_msg(paste("Full dataset collected into R memory:", nrow(df), "rows x", ncol(df), "cols"))

# Coerce types
df <- df %>%
  mutate(
    flight_date    = as.Date(flight_date),
    dep_delay      = as.numeric(dep_delay),
    arr_delay      = as.numeric(arr_delay),
    distance       = as.numeric(distance),
    cancelled      = as.numeric(cancelled),
    diverted       = as.numeric(diverted),
    carrier_delay       = as.numeric(carrier_delay),
    weather_delay       = as.numeric(weather_delay),
    nas_delay           = as.numeric(nas_delay),
    security_delay      = as.numeric(security_delay),
    late_aircraft_delay = as.numeric(late_aircraft_delay),
    month          = month(flight_date),
    week_day       = wday(flight_date, label = TRUE, abbr = TRUE)
  )

# ==============================================================================
# 1. MONTHLY STATISTICAL PROFILE
# ==============================================================================
log_msg("Computing monthly statistical profile...")

monthly_profile <- df %>%
  group_by(month) %>%
  summarise(
    total_flights       = n(),
    cancelled_count     = sum(cancelled == 1, na.rm = TRUE),
    diverted_count      = sum(diverted  == 1, na.rm = TRUE),
    cancel_rate_pct     = round(mean(cancelled == 1, na.rm = TRUE) * 100, 4),
    mean_dep_delay      = round(mean(dep_delay, na.rm = TRUE), 4),
    median_dep_delay    = round(median(dep_delay, na.rm = TRUE), 4),
    sd_dep_delay        = round(sd(dep_delay, na.rm = TRUE), 4),
    p95_dep_delay       = round(quantile(dep_delay, 0.95, na.rm = TRUE), 4),
    mean_arr_delay      = round(mean(arr_delay, na.rm = TRUE), 4),
    median_arr_delay    = round(median(arr_delay, na.rm = TRUE), 4),
    sd_arr_delay        = round(sd(arr_delay, na.rm = TRUE), 4),
    p95_arr_delay       = round(quantile(arr_delay, 0.95, na.rm = TRUE), 4),
    pct_delayed_15plus  = round(mean(dep_delay >= 15, na.rm = TRUE) * 100, 4),
    mean_distance_mi    = round(mean(distance, na.rm = TRUE), 2),
    .groups = "drop"
  )

write.csv(monthly_profile,
          file.path(META_DIR, "statistical_profile.csv"),
          row.names = FALSE)
log_msg(paste("Saved: statistical_profile.csv (", nrow(monthly_profile), "rows )"))

# ==============================================================================
# 2. CARRIER PROFILE
# ==============================================================================
log_msg("Computing carrier-level profile...")

carrier_profile <- df %>%
  group_by(airline) %>%
  summarise(
    total_flights    = n(),
    cancel_rate_pct  = round(mean(cancelled == 1, na.rm = TRUE) * 100, 4),
    mean_dep_delay   = round(mean(dep_delay, na.rm = TRUE), 4),
    mean_arr_delay   = round(mean(arr_delay, na.rm = TRUE), 4),
    p95_arr_delay    = round(quantile(arr_delay, 0.95, na.rm = TRUE), 4),
    pct_delayed_15plus = round(mean(dep_delay >= 15, na.rm = TRUE) * 100, 4),
    carrier_delay_avg  = round(mean(carrier_delay, na.rm = TRUE), 4),
    weather_delay_avg  = round(mean(weather_delay, na.rm = TRUE), 4),
    .groups = "drop"
  ) %>%
  arrange(desc(total_flights))

write.csv(carrier_profile,
          file.path(META_DIR, "carrier_profile.csv"),
          row.names = FALSE)
log_msg(paste("Saved: carrier_profile.csv (", nrow(carrier_profile), "carriers )"))

# ==============================================================================
# 3. TOP-ROUTE PROFILE (Top 50 by volume)
# ==============================================================================
log_msg("Computing route-level profile (top 50 routes)...")

route_profile <- df %>%
  mutate(route = paste0(origin, "-", dest)) %>%
  group_by(route, origin, dest) %>%
  summarise(
    total_flights   = n(),
    mean_arr_delay  = round(mean(arr_delay, na.rm = TRUE), 4),
    cancel_rate_pct = round(mean(cancelled == 1, na.rm = TRUE) * 100, 4),
    mean_distance   = round(mean(distance, na.rm = TRUE), 1),
    .groups = "drop"
  ) %>%
  arrange(desc(total_flights)) %>%
  slice_head(n = 50)

write.csv(route_profile,
          file.path(META_DIR, "route_profile.csv"),
          row.names = FALSE)
log_msg(paste("Saved: route_profile.csv (top", nrow(route_profile), "routes)"))

# ==============================================================================
# 4. VISUALISATIONS
# ==============================================================================
theme_bts <- theme_minimal(base_size = 13) +
  theme(
    plot.title    = element_text(face = "bold", size = 15),
    plot.subtitle = element_text(color = "grey50"),
    panel.grid.minor = element_blank(),
    legend.position  = "bottom"
  )

# ── Plot 1: Monthly flight volume ──────────────────────────────────────────────
log_msg("Generating Plot 1: Monthly flight volume...")
p1 <- ggplot(monthly_profile, aes(x = factor(month), y = total_flights / 1000)) +
  geom_col(fill = "#2563EB", width = 0.7) +
  geom_text(aes(label = comma(total_flights)), vjust = -0.5, size = 3.5) +
  scale_y_continuous(labels = function(x) paste0(x, "K")) +
  labs(title    = "BTS 2025 — Monthly Flight Volume",
       subtitle = "U.S. Domestic Scheduled Airline Operations",
       x = "Month", y = "Flights (thousands)") +
  theme_bts
ggsave(file.path(PLOT_DIR, "01_monthly_volume.png"), p1, width = 10, height = 6, dpi = 150)

# ── Plot 2: Mean departure delay by month ──────────────────────────────────────
log_msg("Generating Plot 2: Mean departure delay by month...")
p2 <- ggplot(monthly_profile, aes(x = factor(month), y = mean_dep_delay)) +
  geom_col(aes(fill = mean_dep_delay), show.legend = FALSE, width = 0.7) +
  scale_fill_gradient(low = "#22C55E", high = "#EF4444") +
  geom_text(aes(label = round(mean_dep_delay, 1)), vjust = -0.5, size = 3.5) +
  labs(title    = "BTS 2025 — Mean Departure Delay by Month",
       subtitle = "Minutes of average departure delay (non-cancelled flights)",
       x = "Month", y = "Mean Dep Delay (min)") +
  theme_bts
ggsave(file.path(PLOT_DIR, "02_monthly_dep_delay.png"), p2, width = 10, height = 6, dpi = 150)

# ── Plot 3: Cancellation rate by month ────────────────────────────────────────
log_msg("Generating Plot 3: Cancellation rate by month...")
p3 <- ggplot(monthly_profile, aes(x = factor(month), y = cancel_rate_pct)) +
  geom_line(group = 1, color = "#EF4444", linewidth = 1.2) +
  geom_point(color = "#EF4444", size = 3) +
  geom_text(aes(label = paste0(cancel_rate_pct, "%")), vjust = -1, size = 3.5) +
  labs(title    = "BTS 2025 — Monthly Cancellation Rate",
       subtitle = "% of scheduled flights cancelled each month",
       x = "Month", y = "Cancellation Rate (%)") +
  theme_bts
ggsave(file.path(PLOT_DIR, "03_monthly_cancel_rate.png"), p3, width = 10, height = 6, dpi = 150)

# ── Plot 4: Top 15 carriers by mean arrival delay ─────────────────────────────
log_msg("Generating Plot 4: Carrier mean arrival delay...")
p4 <- carrier_profile %>%
  slice_head(n = 15) %>%
  ggplot(aes(x = reorder(airline, -mean_arr_delay), y = mean_arr_delay, fill = mean_arr_delay)) +
  geom_col(show.legend = FALSE, width = 0.7) +
  scale_fill_gradient(low = "#22C55E", high = "#EF4444") +
  geom_text(aes(label = round(mean_arr_delay, 1)), hjust = -0.2, size = 3.5) +
  coord_flip() +
  labs(title    = "BTS 2025 — Mean Arrival Delay by Carrier (Top 15)",
       subtitle = "Airlines ranked by average arrival delay (minutes)",
       x = "Carrier", y = "Mean Arr Delay (min)") +
  theme_bts
ggsave(file.path(PLOT_DIR, "04_carrier_arr_delay.png"), p4, width = 10, height = 7, dpi = 150)

# ── Plot 5: Delay cause breakdown (stacked bar by month) ──────────────────────
log_msg("Generating Plot 5: Delay cause breakdown...")
delay_causes <- df %>%
  filter(cancelled == 0) %>%
  group_by(month) %>%
  summarise(
    Carrier      = round(mean(carrier_delay,       na.rm = TRUE), 2),
    Weather      = round(mean(weather_delay,       na.rm = TRUE), 2),
    NAS          = round(mean(nas_delay,           na.rm = TRUE), 2),
    Security     = round(mean(security_delay,      na.rm = TRUE), 2),
    Late_Aircraft= round(mean(late_aircraft_delay, na.rm = TRUE), 2),
    .groups = "drop"
  )
library(tidyr)
delay_long <- delay_causes %>%
  pivot_longer(-month, names_to = "cause", values_to = "avg_min")

p5 <- ggplot(delay_long, aes(x = factor(month), y = avg_min, fill = cause)) +
  geom_col(position = "stack", width = 0.75) +
  scale_fill_manual(values = c(
    "Carrier" = "#2563EB", "Weather" = "#7C3AED",
    "NAS" = "#F59E0B", "Security" = "#EF4444", "Late_Aircraft" = "#10B981"
  )) +
  labs(title    = "BTS 2025 — Average Delay by Cause per Month",
       subtitle = "Stacked average minutes of each delay category",
       x = "Month", y = "Avg Delay Contribution (min)", fill = "Cause") +
  theme_bts
ggsave(file.path(PLOT_DIR, "05_delay_causes_stacked.png"), p5, width = 12, height = 7, dpi = 150)

# ==============================================================================
# DONE
# ==============================================================================
log_msg("====================================================================")
log_msg("STAGE 7 COMPLETE")
log_msg(paste("  statistical_profile.csv :", nrow(monthly_profile), "months"))
log_msg(paste("  carrier_profile.csv     :", nrow(carrier_profile), "carriers"))
log_msg(paste("  route_profile.csv       :", nrow(route_profile), "routes"))
log_msg(paste("  Plots saved to          :", PLOT_DIR))
log_msg("====================================================================")
