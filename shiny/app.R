# ==============================================================================
# shiny/app.R
# Stage 11: Interactive R Shiny Analytics Dashboard
# Technology: R 4.x (shiny, ggplot2, dplyr, scales)
# ==============================================================================

.libPaths(c("C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(shiny)
  library(ggplot2)
  library(dplyr)
  library(scales)
})

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR <- "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics"
META_DIR <- file.path(BASE_DIR, "dataset", "metadata")

# ── Load Precomputed Metadata ──────────────────────────────────────────────────
stat_profile <- if (file.exists(file.path(META_DIR, "statistical_profile.csv"))) {
  read.csv(file.path(META_DIR, "statistical_profile.csv"))
} else { data.frame() }

carrier_profile <- if (file.exists(file.path(META_DIR, "carrier_profile.csv"))) {
  read.csv(file.path(META_DIR, "carrier_profile.csv"))
} else { data.frame() }

route_profile <- if (file.exists(file.path(META_DIR, "route_profile.csv"))) {
  read.csv(file.path(META_DIR, "route_profile.csv"))
} else { data.frame() }

subsets_summary <- if (file.exists(file.path(META_DIR, "experiment_dataset_summary.csv"))) {
  read.csv(file.path(META_DIR, "experiment_dataset_summary.csv"))
} else { data.frame() }

# Month Names Map
month_names <- c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
if (nrow(stat_profile) > 0) {
  stat_profile$month_name <- factor(month_names[stat_profile$month], levels = month_names)
}

# ── Custom CSS Style ───────────────────────────────────────────────────────────
custom_css <- "
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f8f9fa; }
.navbar-default { background-color: #1a252f; border-color: #2c3e50; }
.navbar-default .navbar-brand { color: #ecf0f1; font-weight: bold; }
.navbar-default .navbar-nav > li > a { color: #bdc3c7; }
.navbar-default .navbar-nav > .active > a { background-color: #2c3e50 !important; color: #ffffff !important; }
.metric-card {
  background: white; border-radius: 8px; padding: 18px 22px; margin-bottom: 20px;
  box-shadow: 0 2px 6px rgba(0,0,0,0.06); border-left: 5px solid #3498db;
}
.metric-card.danger { border-left-color: #e74c3c; }
.metric-card.warning { border-left-color: #f39c12; }
.metric-card.success { border-left-color: #2ecc71; }
.metric-card.purple { border-left-color: #9b59b6; }
.metric-num { font-size: 28px; font-weight: bold; color: #2c3e50; margin: 4px 0; }
.metric-label { font-size: 13px; text-transform: uppercase; color: #7f8c8d; font-weight: 600; }
.panel-custom { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 6px rgba(0,0,0,0.06); margin-bottom: 25px; }
.table-custom { font-size: 13px; }
"

# ==============================================================================
# UI DEFINITION
# ==============================================================================
ui <- fluidPage(
  tags$head(tags$style(HTML(custom_css))),
  
  navbarPage(
    title = "Airline Delay Analytics (BTS 2025)",
    id = "nav",
    
    # ── TAB 1: OVERVIEW ────────────────────────────────────────────────────────
    tabPanel("Overview & KPIs",
      fluidRow(
        column(12,
          h2("National Flight Delay & Punctuality Dashboard (2025)"),
          p("End-to-End Big Data Analytics across 7.00 Million BTS On-Time Reporting Flight Records."),
          hr()
        )
      ),
      
      # Top KPI Metrics Cards
      fluidRow(
        column(3,
          div(class = "metric-card",
            div(class = "metric-label", "Total Clean Flights Analyzed"),
            div(class = "metric-num", format(sum(stat_profile$total_flights), big.mark = ",")),
            p("100% Validated & Profiling Clean")
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
            p("Punctuality threshold (<15 min: 78.4%)")
          )
        )
      ),
      
      # Charts Row
      fluidRow(
        column(7,
          div(class = "panel-custom",
            h4("Monthly Flight Volume (2025)"),
            plotOutput("plot_monthly_volume", height = "320px")
          )
        ),
        column(5,
          div(class = "panel-custom",
            h4("Monthly Cancellation Rate (%)"),
            plotOutput("plot_monthly_cancel", height = "320px")
          )
        )
      ),
      
      # Monthly Summary Table
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
    tabPanel("Seasonality & Delay Distributions",
      fluidRow(
        column(12,
          h3("Seasonal Volatility & Departure vs Arrival Delays"),
          p("Comparing mean, median, and 95th percentile delay evolution across calendar months."),
          hr()
        )
      ),
      fluidRow(
        column(6,
          div(class = "panel-custom",
            h4("Mean Departure vs Arrival Delay by Month"),
            plotOutput("plot_delay_trends", height = "350px")
          )
        ),
        column(6,
          div(class = "panel-custom",
            h4("95th Percentile Delay (Extreme Delay Thresholds)"),
            plotOutput("plot_p95_delay", height = "350px")
          )
        )
      ),
      fluidRow(
        column(12,
          div(class = "panel-custom",
            h4("Key Findings on 2025 Seasonality:"),
            tags$ul(
              tags$li("Summer Delay Surge: July experienced the highest mean departure delay (21.51 min) and arrival delay (17.35 min), with 95th percentile arrival delay reaching 124 minutes."),
              tags$li("Autumn Stability: September was the most punctual month of 2025, with mean departure delay falling to 8.11 minutes and cancellation rate dropping to 0.51%."),
              tags$li("Winter Weather Disruption: January witnessed the highest monthly cancellation rate (3.02%, 16,312 flights) driven by severe winter storm systems.")
            )
          )
        )
      )
    ),
    
    # ── TAB 3: CARRIER BENCHMARKING ────────────────────────────────────────────
    tabPanel("Carrier Benchmarking",
      fluidRow(
        column(12,
          h3("Reporting Carrier Performance & Ranking"),
          p("Comparative analysis of major US airlines by volume, on-time rates, and delay causes."),
          hr()
        )
      ),
      fluidRow(
        column(4,
          div(class = "panel-custom",
            h4("Top Carriers by Flight Volume"),
            plotOutput("plot_carrier_volume", height = "350px")
          )
        ),
        column(4,
          div(class = "panel-custom",
            h4("Carrier Cancellation Rate (%)"),
            plotOutput("plot_carrier_cancel", height = "350px")
          )
        ),
        column(4,
          div(class = "panel-custom",
            h4("Mean Arrival Delay by Carrier"),
            plotOutput("plot_carrier_delay", height = "350px")
          )
        )
      ),
      fluidRow(
        column(12,
          div(class = "panel-custom",
            h4("Carrier Performance Data Table"),
            tableOutput("table_carrier_profile")
          )
        )
      )
    ),
    
    # ── TAB 4: ROUTE EXPLORER ──────────────────────────────────────────────────
    tabPanel("Route Delays",
      fluidRow(
        column(12,
          h3("High-Density Route & Corridor Performance"),
          p("Analysis of the top 50 domestic flight routes in the United States."),
          hr()
        )
      ),
      fluidRow(
        column(6,
          div(class = "panel-custom",
            h4("Top 15 Busiest Routes by Flight Volume"),
            plotOutput("plot_top_routes_volume", height = "380px")
          )
        ),
        column(6,
          div(class = "panel-custom",
            h4("Worst Delayed High-Density Routes (Arrival Delay Mins)"),
            plotOutput("plot_top_routes_delay", height = "380px")
          )
        )
      ),
      fluidRow(
        column(12,
          div(class = "panel-custom",
            h4("Top Routes Performance Table"),
            tableOutput("table_route_profile")
          )
        )
      )
    ),
    
    # ── TAB 5: EXPERIMENTAL SUBSETS ───────────────────────────────────────────
    tabPanel("Big Data Subsets & Benchmarks",
      fluidRow(
        column(12,
          h3("Multi-Scale Experimental Benchmarking Datasets"),
          p("Nested chronological slices and stratified datasets generated for MapReduce, Hive, and Kafka scalability studies."),
          hr()
        )
      ),
      fluidRow(
        column(12,
          div(class = "panel-custom",
            h4("Generated Experimental Subsets (Dual Parquet + CSV Format)"),
            tableOutput("table_subsets")
          )
        )
      ),
      fluidRow(
        column(6,
          div(class = "panel-custom",
            h4("Scalability Benchmark Design"),
            tags$ul(
              tags$li("flights_100k (100K rows, ~12.6 MB): Rapid prototyping, local MapReduce testing & SerDe verification."),
              tags$li("flights_500k (500K rows, ~62.8 MB): Medium-tier scaling, window aggregation sanity testing."),
              tags$li("flights_1m (1M rows, ~125.8 MB): Nested chronological slice for throughput thresholding."),
              tags$li("flights_1m_proportional (1M rows, ~125.9 MB): Seasonally stratified sample preserving natural monthly traffic distribution across all 365 days."),
              tags$li("flights_1m_equal (1M rows, ~125.9 MB): Uniform monthly allocation (83,333 flights/month) to isolate seasonal volume volatility.")
            )
          )
        ),
        column(6,
          div(class = "panel-custom",
            h4("Distributed Processing Engine Responsibilities"),
            tags$ul(
              tags$li("Apache Hadoop MapReduce (Java 8): Distributed batch processing of Workloads W1 (Carrier Delays), W2 (Hourly Congestion), and W4 (Route Summary)."),
              tags$li("Apache Hive 1.2.1 on Tez: Columnar Parquet external table querying with vectorized execution."),
              tags$li("Apache Kafka 3.6 / 0.10: High-throughput real-time streaming ingestion with at-least-once crash-safe checkpointing.")
            )
          )
        )
      )
    ),
    
    # ── TAB 6: ARCHITECTURE & STREAMING ────────────────────────────────────────
    tabPanel("Architecture & Consistency",
      fluidRow(
        column(12,
          h3("Batch vs Stream Consistency & Architecture Protocol"),
          p("Formal hybrid Lambda reconciliation evaluating streaming window state against batch ground truth."),
          hr()
        )
      ),
      fluidRow(
        column(6,
          div(class = "panel-custom",
            h4("4-Phase Crash-Safe Checkpointing Protocol (Kafka Consumer)"),
            tags$ol(
              tags$li(strong("Phase 1 (Pause Ingestion): "), "Consumer temporarily pauses message consumption across assigned topic partitions."),
              tags$li(strong("Phase 2 (State Serialization): "), "Active tumbling window aggregations and seen event_id hashes are serialized to temporary snapshot (state_ckpt.tmp) and atomically renamed."),
              tags$li(strong("Phase 3 (Synchronous Offset Commit): "), "Kafka consumer synchronously commits offsets via consumer.commitSync()."),
              tags$li(strong("Phase 4 (Idempotent Publication): "), "Aggregated window sink files are committed atomically to HDFS / local storage; consumption resumes.")
            )
          )
        ),
        column(6,
          div(class = "panel-custom",
            h4("Deterministic Key Invariants & Row Conservation"),
            tags$p("Every record is governed by strict mathematical accounting:"),
            tags$blockquote("N_raw = N_retained + N_quarantine + N_exact_duplicates"),
            tags$p("Deterministic SHA-256 Event Key:"),
            tags$code("event_id = SHA-256(flight_date || airline || flight_num || origin || dest || crs_dep)"),
            tags$p(style = "margin-top: 10px;", "Guarantees idempotent updates and eliminates double-counting during Kafka broker replay or consumer crash recovery.")
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
      labs(x = "Month (2025)", y = "Flight Volume (Thousands)", title = "Total Monthly Flights")
  })

  output$plot_monthly_cancel <- renderPlot({
    req(nrow(stat_profile) > 0)
    ggplot(stat_profile, aes(x = month_name, y = cancel_rate_pct)) +
      geom_bar(stat = "identity", fill = "#e74c3c", width = 0.65) +
      geom_text(aes(label = sprintf("%.2f%%", cancel_rate_pct)), vjust = -0.4, size = 3.5, fontface = "bold") +
      scale_y_continuous(limits = c(0, 3.8)) +
      theme_minimal(base_size = 12) +
      labs(x = "Month (2025)", y = "Cancellation Rate (%)", title = "Monthly Cancellation Rate")
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
      labs(x = "Month (2025)", y = "Delay (Minutes)", color = "Metric", title = "Departure vs Arrival Delay Evolution")
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
      labs(x = "Month (2025)", y = "95th Percentile Delay (Minutes)", color = "Metric", title = "Extreme Tail Delay (95th Percentile)")
  })

  # ── TAB 3 PLOTS ──
  output$plot_carrier_volume <- renderPlot({
    req(nrow(carrier_profile) > 0)
    top_c <- head(arrange(carrier_profile, desc(total_flights)), 10)
    ggplot(top_c, aes(x = reorder(airline, total_flights), y = total_flights / 1000)) +
      geom_bar(stat = "identity", fill = "#2c3e50") +
      coord_flip() +
      theme_minimal(base_size = 12) +
      labs(x = "Carrier", y = "Flights (Thousands)", title = "Top 10 Carriers by Volume")
  })

  output$plot_carrier_cancel <- renderPlot({
    req(nrow(carrier_profile) > 0)
    top_c <- head(arrange(carrier_profile, desc(total_flights)), 10)
    ggplot(top_c, aes(x = reorder(airline, cancel_rate_pct), y = cancel_rate_pct)) +
      geom_bar(stat = "identity", fill = "#e74c3c") +
      coord_flip() +
      theme_minimal(base_size = 12) +
      labs(x = "Carrier", y = "Cancellation Rate (%)", title = "Carrier Cancellation Rate")
  })

  output$plot_carrier_delay <- renderPlot({
    req(nrow(carrier_profile) > 0)
    top_c <- head(arrange(carrier_profile, desc(total_flights)), 10)
    ggplot(top_c, aes(x = reorder(airline, mean_arr_delay), y = mean_arr_delay)) +
      geom_bar(stat = "identity", fill = "#f39c12") +
      coord_flip() +
      theme_minimal(base_size = 12) +
      labs(x = "Carrier", y = "Mean Arr Delay (Mins)", title = "Mean Arrival Delay by Carrier")
  })

  output$table_carrier_profile <- renderTable({
    req(nrow(carrier_profile) > 0)
    carrier_profile %>%
      arrange(desc(total_flights)) %>%
      select(Carrier = airline,
             `Total Flights` = total_flights,
             `Cancel Rate (%)` = cancel_rate_pct,
             `Mean Dep Delay` = mean_dep_delay,
             `Mean Arr Delay` = mean_arr_delay,
             `P95 Arr Delay` = p95_arr_delay,
             `Delayed 15+ Min (%)` = pct_delayed_15plus,
             `Carrier Delay Avg` = carrier_delay_avg,
             `Weather Delay Avg` = weather_delay_avg) %>%
      mutate(`Total Flights` = format(`Total Flights`, big.mark = ","))
  }, striped = TRUE, hover = TRUE, bordered = TRUE)

  # ── TAB 4 PLOTS ──
  output$plot_top_routes_volume <- renderPlot({
    req(nrow(route_profile) > 0)
    top_r <- head(arrange(route_profile, desc(total_flights)), 15)
    ggplot(top_r, aes(x = reorder(route, total_flights), y = total_flights)) +
      geom_bar(stat = "identity", fill = "#16a085") +
      coord_flip() +
      theme_minimal(base_size = 12) +
      labs(x = "Route", y = "Annual Flights", title = "Top 15 Routes by Volume")
  })

  output$plot_top_routes_delay <- renderPlot({
    req(nrow(route_profile) > 0)
    worst_r <- head(arrange(route_profile, desc(mean_arr_delay)), 15)
    ggplot(worst_r, aes(x = reorder(route, mean_arr_delay), y = mean_arr_delay)) +
      geom_bar(stat = "identity", fill = "#d35400") +
      coord_flip() +
      theme_minimal(base_size = 12) +
      labs(x = "Route", y = "Mean Arr Delay (Mins)", title = "Worst Delayed High-Density Routes")
  })

  output$table_route_profile <- renderTable({
    req(nrow(route_profile) > 0)
    head(route_profile, 20) %>%
      select(Route = route,
             Origin = origin,
             Dest = dest,
             `Total Flights` = total_flights,
             `Mean Arr Delay (min)` = mean_arr_delay,
             `Cancel Rate (%)` = cancel_rate_pct,
             `Distance (mi)` = mean_distance) %>%
      mutate(`Total Flights` = format(`Total Flights`, big.mark = ","))
  }, striped = TRUE, hover = TRUE, bordered = TRUE)

  # ── TAB 5 TABLE ──
  output$table_subsets <- renderTable({
    req(nrow(subsets_summary) > 0)
    subsets_summary %>%
      select(`Subset ID` = subset_id,
             `Record Count` = record_count,
             `Parquet Size (MB)` = parquet_size_bytes,
             `CSV Size (MB)` = csv_size_bytes,
             `Start Date` = start_date,
             `End Date` = end_date,
             `Days Spanned` = calendar_days_spanned,
             `Sampling Design` = sampling_method) %>%
      mutate(`Record Count` = format(`Record Count`, big.mark = ","),
             `Parquet Size (MB)` = sprintf("%.2f MB", `Parquet Size (MB)` / 1024 / 1024),
             `CSV Size (MB)` = sprintf("%.2f MB", `CSV Size (MB)` / 1024 / 1024))
  }, striped = TRUE, hover = TRUE, bordered = TRUE)
}

# ==============================================================================
# LAUNCH APPLICATION
# ==============================================================================
shinyApp(ui = ui, server = server)
