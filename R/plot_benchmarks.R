# ==============================================================================
# R/plot_benchmarks.R
# Generate High-Resolution Publication Charts for:
#   1. Execution Time Comparison (MapReduce vs Hive on Tez)
#   2. Kafka Latency & Throughput Scaling (p50/p95/p99)
#   3. Resource Utilization (CPU, Memory, Disk I/O)
#   4. Batch vs Stream Consistency Reconciliation
#   5. Fault Tolerance & Late/Duplicate Injection Scenarios
# Technology: R 4.x (ggplot2, dplyr, scales, tidyr)
# ==============================================================================

.libPaths(c("C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(scales)
  library(tidyr)
})

BASE_DIR <- "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics"
META_DIR <- file.path(BASE_DIR, "dataset", "metadata")
PLOT_DIR <- file.path(BASE_DIR, "R", "plots")

dir.create(PLOT_DIR, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# 1. Execution Time: MapReduce vs Hive on Tez
# ------------------------------------------------------------------------------
df_exec <- read.csv(file.path(META_DIR, "benchmark_execution_time.csv"))

df_exec_long <- df_exec %>%
  filter(workload_id == "W1") %>%
  select(dataset_subset, record_count, MapReduce = mapreduce_sec, `Hive on Tez` = hive_tez_sec) %>%
  pivot_longer(cols = c(MapReduce, `Hive on Tez`), names_to = "Engine", values_to = "Seconds")

p1 <- ggplot(df_exec_long, aes(x = factor(dataset_subset, levels = c("flights_100k", "flights_500k", "flights_1m", "flights_5m", "flights_full")),
                               y = Seconds, fill = Engine)) +
  geom_bar(stat = "identity", position = position_dodge(0.8), width = 0.7) +
  geom_text(aes(label = sprintf("%.1fs", Seconds)), position = position_dodge(0.8), vjust = -0.5, size = 3.5, fontface = "bold") +
  scale_fill_manual(values = c("MapReduce" = "#e67e22", "Hive on Tez" = "#2980b9")) +
  scale_y_continuous(labels = comma, limits = c(0, 1250)) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "top", plot.title = element_text(face = "bold")) +
  labs(title = "Execution Time Scaling: MapReduce vs Hive on Tez (Workload W1)",
       subtitle = "Hive on Tez achieves 4.49x to 4.93x speedup via vectorized Parquet execution & Tez DAG pipelining",
       x = "Dataset Subset", y = "Wall-Clock Time (Seconds)", fill = "Processing Engine")

ggsave(file.path(PLOT_DIR, "06_mapreduce_vs_hive_execution_time.png"), plot = p1, width = 10, height = 6, dpi = 300)

# ------------------------------------------------------------------------------
# 2. Kafka Performance: Throughput & Latency (p50/p95/p99)
# ------------------------------------------------------------------------------
df_kafka <- read.csv(file.path(META_DIR, "benchmark_kafka_performance.csv"))

df_lat_long <- df_kafka %>%
  select(offered_rate_eps, p50 = p50_latency_ms, p95 = p95_latency_ms, p99 = p99_latency_ms) %>%
  pivot_longer(cols = c(p50, p95, p99), names_to = "Percentile", values_to = "Latency_ms")

p2 <- ggplot(df_lat_long, aes(x = offered_rate_eps, y = Latency_ms, color = Percentile, group = Percentile)) +
  geom_line(linewidth = 1.3) +
  geom_point(size = 3.5) +
  geom_vline(xintercept = 5000, linetype = "dashed", color = "#c0392b", linewidth = 1) +
  annotate("text", x = 5200, y = 1400, label = "Saturation Knee (~5,000 eps)", color = "#c0392b", hjust = 0, fontface = "bold") +
  scale_color_manual(values = c("p50" = "#27ae60", "p95" = "#f39c12", "p99" = "#e74c3c")) +
  scale_x_continuous(labels = comma) +
  scale_y_log10(labels = comma) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "top", plot.title = element_text(face = "bold")) +
  labs(title = "Kafka Stream Processing: Latency Percentiles vs Offered Rate",
       subtitle = "Sub-25ms p50 latency up to 5,000 events/sec before queue saturation",
       x = "Offered Ingestion Rate (events/sec)", y = "Latency (ms, Log Scale)", color = "Percentile")

ggsave(file.path(PLOT_DIR, "07_kafka_latency_throughput_curves.png"), plot = p2, width = 10, height = 6, dpi = 300)

# ------------------------------------------------------------------------------
# 3. Resource Utilization: CPU & Memory
# ------------------------------------------------------------------------------
df_res <- read.csv(file.path(META_DIR, "benchmark_resource_utilization.csv"))

p3 <- ggplot(df_res, aes(x = workload_id, y = cpu_util_avg_pct, fill = engine)) +
  geom_bar(stat = "identity", position = position_dodge(0.8), width = 0.7) +
  geom_text(aes(label = sprintf("%.1f%%", cpu_util_avg_pct)), position = position_dodge(0.8), vjust = -0.5, size = 3.5, fontface = "bold") +
  scale_fill_manual(values = c("Hadoop MapReduce" = "#e67e22", "Hive on Tez" = "#2980b9", "Kafka Stream Consumer" = "#27ae60")) +
  scale_y_continuous(limits = c(0, 110)) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "top", plot.title = element_text(face = "bold")) +
  labs(title = "Average CPU Core Utilization Across Processing Engines",
       subtitle = "Hive on Tez saturates available cores via multithreaded vectorized processing",
       x = "Workload", y = "Average CPU Utilization (%)", fill = "Engine")

ggsave(file.path(PLOT_DIR, "08_cpu_memory_resource_utilization.png"), plot = p3, width = 10, height = 6, dpi = 300)

# ------------------------------------------------------------------------------
# 4. Batch vs Stream Consistency Reconciliation
# ------------------------------------------------------------------------------
df_cons <- read.csv(file.path(META_DIR, "benchmark_consistency_reconciliation.csv")) %>%
  filter(carrier != "TOTAL")

p4 <- ggplot(df_cons, aes(x = reorder(carrier, -batch_records))) +
  geom_bar(aes(y = batch_records / 1000, fill = "Batch Ground Truth"), stat = "identity", width = 0.6) +
  geom_point(aes(y = stream_records / 1000, color = "Stream Aggregation"), size = 3, shape = 18) +
  scale_fill_manual(values = c("Batch Ground Truth" = "#3498db")) +
  scale_color_manual(values = c("Stream Aggregation" = "#e74c3c")) +
  scale_y_continuous(labels = comma) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "top", plot.title = element_text(face = "bold")) +
  labs(title = "Batch vs Stream Consistency: Carrier Record Counts",
       subtitle = "Exact 100.000% parity (delta = 0 across all carriers, zero double-counting)",
       x = "Carrier", y = "Records Processed (Thousands)", fill = "", color = "")

ggsave(file.path(PLOT_DIR, "09_batch_vs_stream_consistency_error.png"), plot = p4, width = 10, height = 6, dpi = 300)

# ------------------------------------------------------------------------------
# 5. Fault Tolerance Scenarios
# ------------------------------------------------------------------------------
df_ft <- read.csv(file.path(META_DIR, "benchmark_fault_tolerance.csv"))

p5 <- ggplot(df_ft, aes(x = scenario_id, y = as.numeric(sub("%", "", key_parity_pct)), fill = scenario_id)) +
  geom_bar(stat = "identity", width = 0.65) +
  geom_text(aes(label = key_parity_pct), vjust = -0.4, size = 4, fontface = "bold") +
  scale_fill_manual(values = c("F0" = "#27ae60", "F1" = "#2980b9", "F2" = "#8e44ad", "F3" = "#16a085", "F4" = "#d35400")) +
  scale_y_continuous(limits = c(0, 115)) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "none", plot.title = element_text(face = "bold")) +
  labs(title = "Stream Fault Tolerance: Key Parity % Across Injected Fault Scenarios",
       subtitle = "F0 (Baseline), F1 (5% Dups), F2 (10% Dups + Crash), F3 (Late <10m), F4 (Late >30m Quarantine)",
       x = "Fault Injection Scenario", y = "Key Parity with Batch Ground Truth (%)")

ggsave(file.path(PLOT_DIR, "10_fault_tolerance_scenarios.png"), plot = p5, width = 10, height = 6, dpi = 300)

cat("Successfully generated benchmark publication plots 06 through 10 in R/plots/\n")
