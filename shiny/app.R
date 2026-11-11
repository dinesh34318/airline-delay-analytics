# ==============================================================================
# shiny/app.R
# Stage 11: Interactive Big Data Analytics Dashboard (BTS 2025)
# Includes:
#   - Overview & Executive KPIs (7.00M records)
#   - Seasonality & Delay Distributions
#   - Carrier & Route Benchmarking
#   - Execution Time: MapReduce vs Hive on Tez
#   - Kafka Performance: Throughput & p50/p95/p99 Latency
#   - Resource Utilization: CPU, Memory & Execution Time
#   - Batch vs Stream Consistency Reconciliation
#   - Fault Tolerance: Late & Duplicate Event Scenarios
# Technology: R 4.x (shiny, ggplot2, dplyr, scales, tidyr)
# ==============================================================================

.libPaths(c("C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(shiny)
  library(ggplot2)
  library(dplyr)
  library(scales)
  library(tidyr)
})

BASE_DIR <- "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics"
META_DIR <- file.path(BASE_DIR, "dataset", "metadata")

# ── Load All Datasets ──────────────────────────────────────────────────────────
load_csv <- function(name) {
  p <- file.path(META_DIR, name)
  if (file.exists(p)) read.csv(p, stringsAsFactors = FALSE) else data.frame()
}

stat_profile     <- load_csv("statistical_profile.csv")
carrier_profile  <- load_csv("carrier_profile.csv")
route_profile    <- load_csv("route_profile.csv")
subsets_summary  <- load_csv("experiment_dataset_summary.csv")
bench_exec       <- load_csv("benchmark_execution_time.csv")
bench_kafka      <- load_csv("benchmark_kafka_performance.csv")
bench_res        <- load_csv("benchmark_resource_utilization.csv")
bench_cons       <- load_csv("benchmark_consistency_reconciliation.csv")
bench_ft         <- load_csv("benchmark_fault_tolerance.csv")

month_names <- c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
if (nrow(stat_profile) > 0) {
  stat_profile$month_name <- factor(month_names[stat_profile$month], levels = month_names)
}

# ── Custom CSS ─────────────────────────────────────────────────────────────────
custom_css <- "
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f9; color: #2c3e50; }
.navbar-default { background-color: #1a252f; border-color: #2c3e50; }
.navbar-default .navbar-brand { color: #ecf0f1 !important; font-weight: bold; font-size: 18px; }
.navbar-default .navbar-nav > li > a { color: #bdc3c7 !important; font-weight: 500; }
.navbar-default .navbar-nav > .active > a { background-color: #34495e !important; color: #ffffff !important; border-bottom: 3px solid #3498db; }
.metric-card {
  background: white; border-radius: 8px; padding: 18px 20px; margin-bottom: 20px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.06); border-left: 5px solid #3498db;
}
.metric-card.danger  { border-left-color: #e74c3c; }
.metric-card.warning { border-left-color: #f39c12; }
.metric-card.success { border-left-color: #2ecc71; }
.metric-card.purple  { border-left-color: #9b59b6; }
.metric-card.navy    { border-left-color: #34495e; }
.metric-num { font-size: 26px; font-weight: bold; color: #2c3e50; margin: 4px 0; }
.metric-label { font-size: 12px; text-transform: uppercase; color: #7f8c8d; font-weight: 700; }
.panel-custom { background: white; border-radius: 8px; padding: 22px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); margin-bottom: 25px; }
.table-custom { font-size: 13px; }
.badge-status { padding: 4px 10px; border-radius: 4px; font-size: 12px; font-weight: bold; }
.badge-pass { background-color: #d4edda; color: #155724; }
.badge-warn { background-color: #fff3cd; color: #856404; }
"

# ==============================================================================
# UI DEFINITION
# ==============================================================================
ui <- fluidPage(
  tags$head(tags$style(HTML(custom_css))),
  
  navbarPage(
    title = "Airline Delay Analytics (BTS 2025)",
    id = "nav",
    
    # ── TAB 1: OVERVIEW & KPIS ─────────────────────────────────────────────────
    tabPanel("Overview & KPIs",
      fluidRow(
        column(12,
          h2("National Flight Delay & Punctuality Dashboard (2025)"),
          p("End-to-End Big Data Analytics across 7,001,618 BTS On-Time Reporting Flight Records."),
          hr()
        )
      ),
      fluidRow(
        column(3,
          div(class = "metric-card",
            div(class = "metric-label", "Total Clean Flights"),
            div(class = "metric-num", format(sum(stat_profile$total_flights), big.mark = ",")),
            p("100% Retained & Validated")
          )
        ),
        column(2,
          div(class = "metric-card danger",
            div(class = "metric-label", "Overall Cancellations"),
            div(class = "metric-num", paste0(round(100 * sum(stat_profile$cancelled_count) / sum(stat_profile$total_flights), 2), "%")),
            p(paste(format(sum(stat_profile$cancelled_count), big.mark = ","), "Flights"))
          )
        ),
        column(2,
          div(class = "metric-card warning",
            div(class = "metric-label", "Mean Dep Delay"),
            div(class = "metric-num", paste0(round(mean(stat_profile$mean_dep_delay), 1), " min")),
            p("Across all reporting flights")
          )
        ),
        column(2,
          div(class = "metric-card purple",
            div(class = "metric-label", "Mean Arr Delay"),
            div(class = "metric-num", paste0(round(mean(stat_profile$mean_arr_delay), 1), " min")),
            p("Active flown flights")
          )
        ),
        column(3,
          div(class = "metric-card success",
            div(class = "metric-label", "15+ Min Delay Rate"),
            div(class = "metric-num", paste0(round(mean(stat_profile$pct_delayed_15plus), 1), "%")),
            p("On-time rate (<15m): 78.4%")
          )
        )
      ),
      fluidRow(
        column(7,
          div(class = "panel-custom",
            h4("Monthly Flight Volume (2025)"),
            plotOutput("plot_monthly_volume", height = "300px")
          )
        ),
        column(5,
          div(class = "panel-custom",
            h4("Monthly Cancellation Rate (%)"),
            plotOutput("plot_monthly_cancel", height = "300px")
          )
        )
      ),
      fluidRow(
        column(12,
          div(class = "panel-custom",
            h4("Monthly Statistical Profile Summary"),
            tableOutput("table_monthly_profile")
          )
        )
      )
    ),
    
    # ── TAB 2: SEASONALITY & DELAYS ────────────────────────────────────────────
    tabPanel("Seasonality & Trends",
      fluidRow(
        column(12,
          h3("Seasonal Volatility & Delay Evolution"),
          p("Comparing mean, median, and 95th percentile delay evolution across calendar months."),
          hr()
        )
      ),
      fluidRow(
        column(6,
          div(class = "panel-custom",
            h4("Mean Departure vs Arrival Delay by Month"),
            plotOutput("plot_delay_trends", height = "340px")
          )
        ),
        column(6,
          div(class = "panel-custom",
            h4("95th Percentile Delay (Extreme Delay Thresholds)"),
            plotOutput("plot_p95_delay", height = "340px")
          )
        )
      )
    ),

    # ── TAB 3: CARRIERS & ROUTES ───────────────────────────────────────────────
    tabPanel("Carriers & Routes",
      fluidRow(
        column(12,
          h3("Reporting Carrier & High-Density Route Performance"),
          p("Comparative analysis of major US airlines and corridor delays."),
          hr()
        )
      ),
      fluidRow(
        column(4,
          div(class = "panel-custom",
            h4("Top Carriers by Flight Volume"),
            plotOutput("plot_carrier_volume", height = "320px")
          )
        ),
        column(4,
          div(class = "panel-custom",
            h4("Carrier Cancellation Rate (%)"),
            plotOutput("plot_carrier_cancel", height = "320px")
          )
        ),
        column(4,
          div(class = "panel-custom",
            h4("Mean Arrival Delay by Carrier"),
            plotOutput("plot_carrier_delay", height = "320px")
          )
        )
      ),
      fluidRow(
        column(6,
          div(class = "panel-custom",
            h4("Top 15 Busiest Routes by Volume"),
            plotOutput("plot_top_routes_volume", height = "340px")
          )
        ),
        column(6,
          div(class = "panel-custom",
            h4("Worst Delayed High-Density Routes (Arrival Delay Mins)"),
            plotOutput("plot_top_routes_delay", height = "340px")
          )
        )
      )
    ),

    # ── TAB 4: MAPREDUCE VS HIVE ON TEZ ────────────────────────────────────────
    tabPanel("MapReduce vs Hive on Tez",
      fluidRow(
        column(12,
          h3("Execution Time Comparison: Hadoop MapReduce vs Hive on Tez"),
          p("Benchmarking distributed batch performance across Workloads W1 to W4 from 100K to 7.00M records."),
          hr()
        )
      ),
      fluidRow(
        column(3,
          div(class = "metric-card navy",
            div(class = "metric-label", "Average Speedup Factor"),
            div(class = "metric-num", "4.48x"),
            p("Hive on Tez vs MapReduce")
          )
        ),
        column(3,
          div(class = "metric-card success",
            div(class = "metric-label", "Full 7M Run (Hive on Tez)"),
            div(class = "metric-num", "241.6 s"),
            p("4.0 minutes (Workload W1)")
          )
        ),
        column(3,
          div(class = "metric-card danger",
            div(class = "metric-label", "Full 7M Run (MapReduce)"),
            div(class = "metric-num", "1,084.7 s"),
            p("18.1 minutes (Workload W1)")
          )
        ),
        column(3,
          div(class = "metric-card purple",
            div(class = "metric-label", "Primary Speedup Driver"),
            div(class = "metric-num", "Vectorized Tez"),
            p("In-memory pipelined shuffle")
          )
        )
      ),
      fluidRow(
        column(7,
          div(class = "panel-custom",
            h4("Execution Time Scaling Across Dataset Sizes (Workload W1)"),
            plotOutput("plot_mr_vs_hive", height = "340px")
          )
        ),
        column(5,
          div(class = "panel-custom",
            h4("Speedup Factors Across Subsets"),
            plotOutput("plot_speedup", height = "340px")
          )
        )
      ),
      fluidRow(
        column(12,
          div(class = "panel-custom",
            h4("Benchmark Execution Time Data Table (Seconds)"),
            tableOutput("table_bench_exec")
          )
        )
      )
    ),

    # ── TAB 5: KAFKA PERFORMANCE & LATENCY ─────────────────────────────────────
    tabPanel("Kafka Performance & Latency",
      fluidRow(
        column(12,
          h3("Kafka Streaming Performance: Throughput & p50/p95/p99 Latency"),
          p("Real-time stream replay and consumer window aggregation under varying offered ingestion rates."),
          hr()
        )
      ),
      fluidRow(
        column(3,
          div(class = "metric-card success",
            div(class = "metric-label", "Max Sustained Rate"),
            div(class = "metric-num", "5,000 eps"),
            p("Sub-25ms median latency")
          )
        ),
        column(3,
          div(class = "metric-card",
            div(class = "metric-label", "p50 Latency @ 1K eps"),
            div(class = "metric-num", "6.5 ms"),
            p("Optimal micro-batching")
          )
        ),
        column(3,
          div(class = "metric-card warning",
            div(class = "metric-label", "p99 Latency @ 5K eps"),
            div(class = "metric-num", "112.8 ms"),
            p("Pre-saturation boundary")
          )
        ),
        column(3,
          div(class = "metric-card danger",
            div(class = "metric-label", "Saturation Knee Point"),
            div(class = "metric-num", "~6,850 eps"),
            p("Consumer CPU bottleneck")
          )
        )
      ),
      fluidRow(
        column(7,
          div(class = "panel-custom",
            h4("Latency Percentiles vs Offered Ingestion Rate (Log Scale)"),
            plotOutput("plot_kafka_latency", height = "340px")
          )
        ),
        column(5,
          div(class = "panel-custom",
            h4("Offered vs Achieved Throughput Saturation"),
            plotOutput("plot_kafka_throughput", height = "340px")
          )
        )
      ),
      fluidRow(
        column(12,
          div(class = "panel-custom",
            h4("Kafka Performance Metrics Table"),
            tableOutput("table_bench_kafka")
          )
        )
      )
    ),

    # ── TAB 6: RESOURCE UTILIZATION ────────────────────────────────────────────
    tabPanel("Resource Utilization",
      fluidRow(
        column(12,
          h3("Hardware & System Resource Utilization: CPU, Memory, Disk & GC"),
          p("Comparative resource profiling between Hadoop MapReduce, Apache Hive on Tez, and Apache Kafka."),
          hr()
        )
      ),
      fluidRow(
        column(6,
          div(class = "panel-custom",
            h4("Average CPU Core Utilization (%) by Workload"),
            plotOutput("plot_cpu_util", height = "340px")
          )
        ),
        column(6,
          div(class = "panel-custom",
            h4("Peak JVM Heap Allocation (MB)"),
            plotOutput("plot_mem_util", height = "340px")
          )
        )
      ),
      fluidRow(
        column(12,
          div(class = "panel-custom",
            h4("Engine Resource Profiling Summary Table"),
            tableOutput("table_bench_res")
          )
        )
      )
    ),

    # ── TAB 7: BATCH VS STREAM CONSISTENCY ─────────────────────────────────────
    tabPanel("Consistency & Fault Tolerance",
      fluidRow(
        column(12,
          h3("Batch vs Stream Consistency & Fault Tolerance Scenarios"),
          p("Formal reconciliation evaluating streaming window state against batch ground truth under fault injection."),
          hr()
        )
      ),
      fluidRow(
        column(3,
          div(class = "metric-card success",
            div(class = "metric-label", "Record Match Parity"),
            div(class = "metric-num", "100.000%"),
            p("Delta = 0 across all 7M records")
          )
        ),
        column(3,
          div(class = "metric-card success",
            div(class = "metric-label", "Arrival Delay Discrepancy"),
            div(class = "metric-num", "0.00 min"),
            p("Exact mathematical equivalence")
          )
        ),
        column(3,
          div(class = "metric-card navy",
            div(class = "metric-label", "Duplicate Rejection"),
            div(class = "metric-num", "100% Filtered"),
            p("Deterministic SHA-256 event_id")
          )
        ),
        column(3,
          div(class = "metric-card warning",
            div(class = "metric-label", "Out-of-Watermark Handling"),
            div(class = "metric-num", "Quarantined"),
            p("Bounded error under late events")
          )
        )
      ),
      fluidRow(
        column(6,
          div(class = "panel-custom",
            h4("Key Parity % Across Injected Fault Scenarios (F0 to F4)"),
            plotOutput("plot_ft_parity", height = "320px")
          )
        ),
        column(6,
          div(class = "panel-custom",
            h4("Fault Tolerance Injection Scenarios (F0 – F4)"),
            tableOutput("table_bench_ft")
          )
        )
      ),
      fluidRow(
        column(12,
          div(class = "panel-custom",
            h4("Batch Ground Truth vs Streaming Aggregation Reconciliation Table"),
            tableOutput("table_bench_cons")
          )
        )
      )
    )
  )
)

# ==============================================================================
# SERVER LOGIC
# ==============================================================================
server <- function(input, output, session) {

  # ── TAB 1 PLOTS ──
  output$plot_monthly_volume <- renderPlot({
    req(nrow(stat_profile) > 0)
    ggplot(stat_profile, aes(x = month_name, y = total_flights / 1000)) +
      geom_bar(stat = "identity", fill = "#3498db", width = 0.65) +
      geom_text(aes(label = sprintf("%.0fK", total_flights / 1000)), vjust = -0.4, size = 3.5, fontface = "bold") +
      scale_y_continuous(labels = comma, limits = c(0, 700)) +
      theme_minimal(base_size = 12) +
      labs(x = "Month (2025)", y = "Flight Volume (Thousands)")
  })

  output$plot_monthly_cancel <- renderPlot({
    req(nrow(stat_profile) > 0)
    ggplot(stat_profile, aes(x = month_name, y = cancel_rate_pct)) +
      geom_bar(stat = "identity", fill = "#e74c3c", width = 0.65) +
      geom_text(aes(label = sprintf("%.2f%%", cancel_rate_pct)), vjust = -0.4, size = 3.5, fontface = "bold") +
      scale_y_continuous(limits = c(0, 3.8)) +
      theme_minimal(base_size = 12) +
      labs(x = "Month (2025)", y = "Cancellation Rate (%)")
  })

  output$table_monthly_profile <- renderTable({
    req(nrow(stat_profile) > 0)
    stat_profile %>%
      select(Month = month_name,
             `Total Flights` = total_flights,
             `Cancelled` = cancelled_count,
             `Cancel Rate (%)` = cancel_rate_pct,
             `Mean Dep Delay (min)` = mean_dep_delay,
             `Median Dep Delay` = median_dep_delay,
             `P95 Dep Delay` = p95_dep_delay,
             `Mean Arr Delay (min)` = mean_arr_delay,
             `Delayed 15+ Min (%)` = pct_delayed_15plus) %>%
      mutate(`Total Flights` = format(`Total Flights`, big.mark = ","),
             `Cancelled` = format(`Cancelled`, big.mark = ","))
  }, striped = TRUE, hover = TRUE, bordered = TRUE)

  # ── TAB 2 PLOTS ──
  output$plot_delay_trends <- renderPlot({
    req(nrow(stat_profile) > 0)
    ggplot(stat_profile, aes(x = month)) +
      geom_line(aes(y = mean_dep_delay, color = "Mean Dep Delay"), linewidth = 1.2) +
      geom_point(aes(y = mean_dep_delay, color = "Mean Dep Delay"), size = 3) +
      geom_line(aes(y = mean_arr_delay, color = "Mean Arr Delay"), linewidth = 1.2) +
      geom_point(aes(y = mean_arr_delay, color = "Mean Arr Delay"), size = 3) +
      scale_x_continuous(breaks = 1:12, labels = month_names) +
      scale_color_manual(values = c("Mean Dep Delay" = "#e67e22", "Mean Arr Delay" = "#2980b9")) +
      theme_minimal(base_size = 12) +
      labs(x = "Month (2025)", y = "Delay (Minutes)", color = "Metric")
  })

  output$plot_p95_delay <- renderPlot({
    req(nrow(stat_profile) > 0)
    ggplot(stat_profile, aes(x = month)) +
      geom_line(aes(y = p95_dep_delay, color = "P95 Dep Delay"), linewidth = 1.2) +
      geom_point(aes(y = p95_dep_delay, color = "P95 Dep Delay"), size = 3) +
      geom_line(aes(y = p95_arr_delay, color = "P95 Arr Delay"), linewidth = 1.2, linetype = "dashed") +
      geom_point(aes(y = p95_arr_delay, color = "P95 Arr Delay"), size = 3) +
      scale_x_continuous(breaks = 1:12, labels = month_names) +
      scale_color_manual(values = c("P95 Dep Delay" = "#c0392b", "P95 Arr Delay" = "#8e44ad")) +
      theme_minimal(base_size = 12) +
      labs(x = "Month (2025)", y = "95th Percentile Delay (Minutes)", color = "Metric")
  })

  # ── TAB 3 PLOTS ──
  output$plot_carrier_volume <- renderPlot({
    req(nrow(carrier_profile) > 0)
    top_c <- head(arrange(carrier_profile, desc(total_flights)), 10)
    ggplot(top_c, aes(x = reorder(airline, total_flights), y = total_flights / 1000)) +
      geom_bar(stat = "identity", fill = "#2c3e50") + coord_flip() +
      theme_minimal(base_size = 12) + labs(x = "Carrier", y = "Flights (Thousands)")
  })

  output$plot_carrier_cancel <- renderPlot({
    req(nrow(carrier_profile) > 0)
    top_c <- head(arrange(carrier_profile, desc(total_flights)), 10)
    ggplot(top_c, aes(x = reorder(airline, cancel_rate_pct), y = cancel_rate_pct)) +
      geom_bar(stat = "identity", fill = "#e74c3c") + coord_flip() +
      theme_minimal(base_size = 12) + labs(x = "Carrier", y = "Cancellation Rate (%)")
  })

  output$plot_carrier_delay <- renderPlot({
    req(nrow(carrier_profile) > 0)
    top_c <- head(arrange(carrier_profile, desc(total_flights)), 10)
    ggplot(top_c, aes(x = reorder(airline, mean_arr_delay), y = mean_arr_delay)) +
      geom_bar(stat = "identity", fill = "#f39c12") + coord_flip() +
      theme_minimal(base_size = 12) + labs(x = "Carrier", y = "Mean Arr Delay (Mins)")
  })

  output$plot_top_routes_volume <- renderPlot({
    req(nrow(route_profile) > 0)
    top_r <- head(arrange(route_profile, desc(total_flights)), 15)
    ggplot(top_r, aes(x = reorder(route, total_flights), y = total_flights)) +
      geom_bar(stat = "identity", fill = "#16a085") + coord_flip() +
      theme_minimal(base_size = 12) + labs(x = "Route", y = "Annual Flights")
  })

  output$plot_top_routes_delay <- renderPlot({
    req(nrow(route_profile) > 0)
    worst_r <- head(arrange(route_profile, desc(mean_arr_delay)), 15)
    ggplot(worst_r, aes(x = reorder(route, mean_arr_delay), y = mean_arr_delay)) +
      geom_bar(stat = "identity", fill = "#d35400") + coord_flip() +
      theme_minimal(base_size = 12) + labs(x = "Route", y = "Mean Arr Delay (Mins)")
  })

  # ── TAB 4: MAPREDUCE VS HIVE ON TEZ ──
  output$plot_mr_vs_hive <- renderPlot({
    req(nrow(bench_exec) > 0)
    df_long <- bench_exec %>%
      filter(workload_id == "W1") %>%
      select(dataset_subset, MapReduce = mapreduce_sec, `Hive on Tez` = hive_tez_sec) %>%
      pivot_longer(cols = c(MapReduce, `Hive on Tez`), names_to = "Engine", values_to = "Seconds")
    
    ggplot(df_long, aes(x = factor(dataset_subset, levels = c("flights_100k", "flights_500k", "flights_1m", "flights_5m", "flights_full")),
                        y = Seconds, fill = Engine)) +
      geom_bar(stat = "identity", position = position_dodge(0.8), width = 0.7) +
      geom_text(aes(label = sprintf("%.1fs", Seconds)), position = position_dodge(0.8), vjust = -0.4, size = 3.5, fontface = "bold") +
      scale_fill_manual(values = c("MapReduce" = "#e67e22", "Hive on Tez" = "#2980b9")) +
      scale_y_continuous(labels = comma, limits = c(0, 1250)) +
      theme_minimal(base_size = 12) +
      theme(legend.position = "top") +
      labs(x = "Dataset Subset", y = "Wall-Clock Seconds")
  })

  output$plot_speedup <- renderPlot({
    req(nrow(bench_exec) > 0)
    df_speed <- bench_exec %>% filter(workload_id == "W1")
    ggplot(df_speed, aes(x = factor(dataset_subset, levels = c("flights_100k", "flights_500k", "flights_1m", "flights_5m", "flights_full")),
                         y = speedup_factor, group = 1)) +
      geom_line(color = "#27ae60", linewidth = 1.3) +
      geom_point(color = "#27ae60", size = 4) +
      geom_text(aes(label = sprintf("%.2fx", speedup_factor)), vjust = -0.7, fontface = "bold", size = 3.8) +
      scale_y_continuous(limits = c(3.5, 5.5)) +
      theme_minimal(base_size = 12) +
      labs(x = "Dataset Subset", y = "Speedup Factor (MR / Tez)")
  })

  output$table_bench_exec <- renderTable({
    req(nrow(bench_exec) > 0)
    bench_exec %>%
      select(`Dataset Subset` = dataset_subset,
             `Record Count` = record_count,
             `Workload` = workload_id,
             `Workload Name` = workload_name,
             `MapReduce (s)` = mapreduce_sec,
             `Hive on Tez (s)` = hive_tez_sec,
             `Speedup Factor` = speedup_factor) %>%
      mutate(`Record Count` = format(`Record Count`, big.mark = ","),
             `Speedup Factor` = sprintf("%.2fx", `Speedup Factor`))
  }, striped = TRUE, hover = TRUE, bordered = TRUE)

  # ── TAB 5: KAFKA PERFORMANCE ──
  output$plot_kafka_latency <- renderPlot({
    req(nrow(bench_kafka) > 0)
    df_lat <- bench_kafka %>%
      select(offered_rate_eps, p50 = p50_latency_ms, p95 = p95_latency_ms, p99 = p99_latency_ms) %>%
      pivot_longer(cols = c(p50, p95, p99), names_to = "Percentile", values_to = "Latency_ms")
    
    ggplot(df_lat, aes(x = offered_rate_eps, y = Latency_ms, color = Percentile, group = Percentile)) +
      geom_line(linewidth = 1.3) +
      geom_point(size = 3.5) +
      geom_vline(xintercept = 5000, linetype = "dashed", color = "#c0392b", linewidth = 0.9) +
      scale_color_manual(values = c("p50" = "#27ae60", "p95" = "#f39c12", "p99" = "#e74c3c")) +
      scale_x_continuous(labels = comma) +
      scale_y_log10(labels = comma) +
      theme_minimal(base_size = 12) +
      theme(legend.position = "top") +
      labs(x = "Offered Ingestion Rate (events/sec)", y = "Latency (ms, Log Scale)")
  })

  output$plot_kafka_throughput <- renderPlot({
    req(nrow(bench_kafka) > 0)
    ggplot(bench_kafka, aes(x = offered_rate_eps)) +
      geom_line(aes(y = offered_rate_eps, linetype = "Ideal Linear"), color = "#7f8c8d", linewidth = 1) +
      geom_line(aes(y = achieved_throughput_eps, color = "Achieved Throughput"), linewidth = 1.3) +
      geom_point(aes(y = achieved_throughput_eps, color = "Achieved Throughput"), size = 3.5) +
      scale_color_manual(values = c("Achieved Throughput" = "#2980b9")) +
      scale_linetype_manual(values = c("Ideal Linear" = "dashed")) +
      scale_x_continuous(labels = comma) +
      scale_y_continuous(labels = comma) +
      theme_minimal(base_size = 12) +
      theme(legend.position = "top") +
      labs(x = "Offered Ingestion Rate (events/sec)", y = "Achieved Consumer Rate (events/sec)", color = "", linetype = "")
  })

  output$table_bench_kafka <- renderTable({
    req(nrow(bench_kafka) > 0)
    bench_kafka %>%
      select(`Offered Rate (eps)` = offered_rate_eps,
             `Achieved (eps)` = achieved_throughput_eps,
             `p50 (ms)` = p50_latency_ms,
             `p95 (ms)` = p95_latency_ms,
             `p99 (ms)` = p99_latency_ms,
             `Max (ms)` = max_latency_ms,
             `Consumer CPU (%)` = consumer_cpu_pct,
             `Status` = pipeline_status) %>%
      mutate(`Offered Rate (eps)` = format(`Offered Rate (eps)`, big.mark = ","),
             `Achieved (eps)` = format(`Achieved (eps)`, big.mark = ","))
  }, striped = TRUE, hover = TRUE, bordered = TRUE)

  # ── TAB 6: RESOURCE UTILIZATION ──
  output$plot_cpu_util <- renderPlot({
    req(nrow(bench_res) > 0)
    ggplot(bench_res, aes(x = workload_id, y = cpu_util_avg_pct, fill = engine)) +
      geom_bar(stat = "identity", position = position_dodge(0.8), width = 0.7) +
      geom_text(aes(label = sprintf("%.1f%%", cpu_util_avg_pct)), position = position_dodge(0.8), vjust = -0.4, size = 3.5, fontface = "bold") +
      scale_fill_manual(values = c("Hadoop MapReduce" = "#e67e22", "Hive on Tez" = "#2980b9", "Kafka Stream Consumer" = "#27ae60")) +
      scale_y_continuous(limits = c(0, 110)) +
      theme_minimal(base_size = 12) +
      theme(legend.position = "top") +
      labs(x = "Workload", y = "Average CPU Utilization (%)", fill = "Engine")
  })

  output$plot_mem_util <- renderPlot({
    req(nrow(bench_res) > 0)
    ggplot(bench_res, aes(x = workload_id, y = jvm_heap_peak_mb, fill = engine)) +
      geom_bar(stat = "identity", position = position_dodge(0.8), width = 0.7) +
      geom_text(aes(label = sprintf("%d MB", jvm_heap_peak_mb)), position = position_dodge(0.8), vjust = -0.4, size = 3.5, fontface = "bold") +
      scale_fill_manual(values = c("Hadoop MapReduce" = "#e67e22", "Hive on Tez" = "#2980b9", "Kafka Stream Consumer" = "#27ae60")) +
      scale_y_continuous(labels = comma, limits = c(0, 7500)) +
      theme_minimal(base_size = 12) +
      theme(legend.position = "top") +
      labs(x = "Workload", y = "Peak JVM Heap (MB)", fill = "Engine")
  })

  output$table_bench_res <- renderTable({
    req(nrow(bench_res) > 0)
    bench_res %>%
      select(`Engine` = engine,
             `Workload` = workload_id,
             `CPU Avg (%)` = cpu_util_avg_pct,
             `CPU Peak (%)` = cpu_peak_pct,
             `Heap Peak (MB)` = jvm_heap_peak_mb,
             `YARN RAM (GB)` = yarn_ram_gb,
             `Disk Read (MB/s)` = disk_read_mbs,
             `Wall Clock (s)` = wall_clock_sec) %>%
      mutate(`Heap Peak (MB)` = format(`Heap Peak (MB)`, big.mark = ","))
  }, striped = TRUE, hover = TRUE, bordered = TRUE)

  # ── TAB 7: CONSISTENCY & FAULT TOLERANCE ──
  output$plot_ft_parity <- renderPlot({
    req(nrow(bench_ft) > 0)
    df_p <- bench_ft %>% mutate(num_parity = as.numeric(sub("%", "", key_parity_pct)))
    ggplot(df_p, aes(x = scenario_id, y = num_parity, fill = scenario_id)) +
      geom_bar(stat = "identity", width = 0.6) +
      geom_text(aes(label = key_parity_pct), vjust = -0.4, size = 4, fontface = "bold") +
      scale_fill_manual(values = c("F0" = "#27ae60", "F1" = "#2980b9", "F2" = "#8e44ad", "F3" = "#16a085", "F4" = "#d35400")) +
      scale_y_continuous(limits = c(0, 115)) +
      theme_minimal(base_size = 12) +
      theme(legend.position = "none") +
      labs(x = "Scenario ID", y = "Exact Key Parity % with Ground Truth")
  })

  output$table_bench_ft <- renderTable({
    req(nrow(bench_ft) > 0)
    bench_ft %>%
      select(`ID` = scenario_id,
             `Description` = scenario_description,
             `Anomaly Rate` = injected_anomaly_rate,
             `Injected Count` = injected_events_count,
             `Dropped / Filtered` = dedup_dropped_count,
             `Key Parity` = key_parity_pct,
             `Error` = error_pct,
             `Recovery Outcome` = recovery_outcome) %>%
      mutate(`Injected Count` = format(`Injected Count`, big.mark = ","),
             `Dropped / Filtered` = format(`Dropped / Filtered`, big.mark = ","))
  }, striped = TRUE, hover = TRUE, bordered = TRUE)

  output$table_bench_cons <- renderTable({
    req(nrow(bench_cons) > 0)
    bench_cons %>%
      select(`Carrier` = carrier,
             `Batch Records` = batch_records,
             `Stream Records` = stream_records,
             `Record Delta` = record_delta,
             `Batch Delay Sum (m)` = batch_arr_delay_sum,
             `Stream Delay Sum (m)` = stream_arr_delay_sum,
             `Delay Delta (m)` = delay_delta,
             `Relative Error (%)` = relative_error_pct,
             `Key Match Status` = key_match_status) %>%
      mutate(`Batch Records` = format(`Batch Records`, big.mark = ","),
             `Stream Records` = format(`Stream Records`, big.mark = ","),
             `Batch Delay Sum (m)` = format(`Batch Delay Sum (m)`, big.mark = ",", nsmall = 1),
             `Stream Delay Sum (m)` = format(`Stream Delay Sum (m)`, big.mark = ",", nsmall = 1))
  }, striped = TRUE, hover = TRUE, bordered = TRUE)
}

# ==============================================================================
# LAUNCH APPLICATION
# ==============================================================================
shinyApp(ui = ui, server = server)
