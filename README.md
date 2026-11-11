# Performance and Consistency of Batch and Stream Processing for Airline Delay Analytics

## A Hadoop MapReduce, Hive and Kafka Study with R Visualization

### Big Data Research Project (Architecture & Execution Plan Version 3.1)

[![Build & Tests](https://img.shields.io/badge/Test_Suite-22%2F22_Passed-success)](tests/testthat/test_preprocessing.R)
[![Data Engine](https://img.shields.io/badge/R_Runtime-4.3.2-blue)](R/)
[![Batch Processing](https://img.shields.io/badge/Hadoop_MapReduce-2.7.3-orange)](mr_job/)
[![SQL Warehouse](https://img.shields.io/badge/Apache_Hive-1.2.1_on_Tez-yellow)](hive/)
[![Stream Engine](https://img.shields.io/badge/Apache_Kafka-3.6.0_Clients-red)](java/)
[![Interactive Dashboard](https://img.shields.io/badge/R_Shiny-Dashboard-brightgreen)](shiny/)

---

## 1. System Architecture & Technology Stack

This project implements a rigorous, reproducible big data research pipeline for comparative delay analytics on the official Bureau of Transportation Statistics (BTS) Reporting Carrier On-Time Performance dataset (calendar year 2025).

```
+-----------------------------------------------------------------------------------+
|                        Official BTS 2025 Flight Data                              |
|                   (12 Monthly Reporting Carrier Archives)                         |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                        R Data Engineering Pipeline                                |
|  R 4.x: arrow | dplyr | ggplot2 | data.table | lubridate | digest | testthat          |
|  - Pre-download Availability Probe      - Monthly & Chunk-wise Cleansing          |
|  - Resilient Download & SHA-256 Hashes  - 2400 Rollover & Time Validation         |
|  - Safe Archive Extraction              - Deterministic SHA-256 Event IDs         |
|  - Data Profiling & Schema Mapping      - Local Airport Clock vs UTC Normalization|
|  - Strict Quarantine Accounting         - Validity Flags (is_valid_arr/dep)       |
|  - Dynamic Memory Telemetry Logging     - Proportional & Equal Stratified Datasets|
+--------------------+------------------------------------+-------------------------+
                     |                                    |
                     v                                    v
       +----------------------------+      +----------------------------+
       |   Cleaned CSV Datasets     |      |   Cleaned Parquet Datasets |
       | (MapReduce / HDFS Format)  |      |   (Hive on Tez Format)     |
       +--------------+-------------+      +--------------+-------------+
                      |                                   |
                      +-----------------+-----------------+
                                        |
                                        v
+-----------------------------------------------------------------------------------+
|               Distributed Master Storage: Hadoop HDFS (HDP 2.5 Sandbox)           |
|                 /airline/batch/csv/  and  /airline/batch/parquet/                 |
+--------------------+------------------------------------+-------------------------+
                     |                                    |
                     v                                    v
+--------------------------------------+  +-----------------------------------------+
|        Batch Processing              |  |         Stream Processing               |
|  - Java 8 MapReduce Jobs (W1-W4)     |  |  - Apache Kafka Broker                  |
|  - Apache Hive 1.2.1 on Tez          |  |  - Java Kafka Producer (Fault Injector) |
|    (External Tables W1-W4)           |  |  - Java Kafka Consumer / Stream Engine  |
|                                      |  |    (At-Least-Once Delivery Architecture |
|                                      |  |     Tumbling Windows, Atomic Checkpoints|
|                                      |  |     Durable Dedup Store, Seek Recovery) |
+--------------------+-----------------+  +-------------------+---------------------+
                     |                                        |
                     +--------------------+-------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                         Hybrid Reconciliation Layer                               |
|        Exact Consistency Verification: Batch Ground Truth vs Streaming State       |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                  R Statistical Analysis & Interactive Shiny Dashboard             |
|            - Execution Time Scaling (W1-W4) by Dataset Size (100K-5M)             |
|            - End-to-End Latency & Throughput Distributions                        |
|            - Consistency Error Under Late/Duplicate Injections                    |
|            - Flight Delay Patterns by Carrier and Hour (ggplot2 & Shiny App)      |
+-----------------------------------------------------------------------------------+
```

---

## 2. Project Directory Structure

```text
Airline-Delay-Analytics/
├── dataset/
│   ├── raw/
│   │   ├── monthly_zip/            # Original immutable BTS 2025 ZIP archives
│   │   └── monthly_csv/            # Extracted immutable raw CSV files
│   ├── processed/
│   │   ├── clean_csv/              # 12 monthly cleaned CSVs (for MapReduce / HDFS)
│   │   ├── parquet/                # 12 monthly cleaned Parquet files (for Hive external tables)
│   │   ├── quarantine/             # Records failing quality validations
│   │   └── subsets/                # 100K, 500K, 1M, 1M-Proportional, 1M-Equal Subsets
│   └── metadata/                   # Auditable logs, summaries, manifests & profiling
│       ├── download_manifest.csv
│       ├── dataset_dictionary.csv
│       ├── data_quality_report.csv
│       ├── cleanse_report.csv
│       ├── statistical_profile.csv
│       ├── carrier_profile.csv
│       ├── route_profile.csv
│       └── experiment_dataset_summary.csv
├── hive/                           # Stage 8: Apache Hive Data Warehousing
│   └── scripts/
│       ├── 01_create_tables.hql    # External tables pointing to Parquet files on HDFS
│       └── 02_analytics_queries.hql# Vectorized Tez queries (monthly, carrier, route stats)
├── java/                           # Stage 9: Apache Kafka Streaming Module
│   ├── pom.xml                     # Maven build with shaded fat JAR
│   └── src/main/java/com/airline/kafka/
│       ├── FlightEventProducer.java# Paced CSV-to-JSON Kafka event publisher
│       └── FlightEventConsumer.java# Real-time tumbling window aggregation engine
├── mr_job/                         # Stage 10: Apache Hadoop MapReduce Batch Analytics Engine
│   ├── pom.xml                     # Java 1.8 MapReduce build configuration
│   ├── README.md                   # Workload instructions and HDFS execution commands
│   └── src/main/java/com/airline/mr/
│       ├── model/FlightRecordWritable.java # High-performance additive metric Writable
│       ├── W1_CarrierDelayJob.java         # Workload W1: Carrier Delay & Volume
│       ├── W2_AirportHourlyDelayJob.java   # Workload W2: Airport Hourly Congestion
│       ├── W4_RouteDelayJob.java           # Workload W4: Route Delays & Distances
│       ├── AirportHeatmapJob.java          # Monthly Airport-Pair Heatmap Matrix
│       └── AirlineBatchDriver.java         # Unified CLI driver
├── shiny/                          # Stage 11: Interactive R Shiny Analytics Dashboard
│   └── app.R                       # Full interactive dashboard with KPI cards & 6 tabs
├── R/                              # Modular R Data Engineering & Visualization Scripts
│   ├── stage7_statistical_profiling.R # Statistical profiling and ggplot2 exports
│   ├── generate_subsets.R          # Dual Parquet/CSV chronological & stratified generator
│   └── plots/                      # Generated research charts (PNG)
│       ├── 01_monthly_volume.png
│       ├── 02_monthly_dep_delay.png
│       ├── 03_monthly_cancel_rate.png
│       ├── 04_carrier_arr_delay.png
│       └── 05_delay_causes_stacked.png
├── tests/                          # Automated Unit & Invariant Test Suite
│   ├── testthat.R                  # Master test runner
│   └── testthat/
│       └── test_preprocessing.R    # Formal tests TEST-01 through TEST-07
└── README.md                       # Complete system documentation
```

---

## 3. Stages & Implementation Overview

| Stage | Subsystem | Description | Primary Technology | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Stage 1** | Ingestion Audit | Pre-download availability probe of BTS 2025 monthly ZIP archives | Python / HTTP | ✅ Completed |
| **Stage 2** | Raw Ingestion | Resumable HTTP download with SHA-256 checksum validation | Python / SHA-256 | ✅ Completed |
| **Stage 3** | Archive Extraction | Safe extraction with path-traversal prevention | Python / ZIP | ✅ Completed |
| **Stage 4** | Profiling | Raw schema profiling, types, dictionary, encoding audit | Python / Metadata | ✅ Completed |
| **Stage 5** | Data Quality | Anomaly audit, 2400-rollover detection, extreme delays | Python / Audit | ✅ Completed |
| **Stage 6** | Cleansing & Parquet | Chunk-wise cleaning, rollover fixes, SHA-256 `event_id`, Parquet & Clean CSV export (7,001,618 clean records) | Python / PyArrow | ✅ Completed |
| **Stage 7** | Statistical Profiling | Distribution analysis, carrier/route profiling, ggplot2 visualisations | R 4.x / Arrow | ✅ Completed |
| **Stage 8A**| Multi-Scale Subsets | Nested chronological (100K, 500K, 1M) and Stratified (1M-Proportional, 1M-Equal) datasets | R 4.x / Arrow | ✅ Completed |
| **Stage 8B**| Hive on Tez | External Parquet tables & analytical queries (`flights_clean`, `monthly_stats`, `carrier_stats`, `route_stats`) | Apache Hive / Tez | ✅ Completed |
| **Stage 9** | Kafka Streaming | High-throughput JSON producer and crash-safe real-time tumbling window consumer | Java / Kafka 3.6 | ✅ Completed |
| **Stage 10**| Hadoop MapReduce | Distributed batch MapReduce jobs (W1, W2, W4, and Airport-Pair Delay Heatmap) | Java 8 / Hadoop MR | ✅ Completed |
| **Stage 11**| Shiny Dashboard | Interactive 6-tab analytics dashboard for visual exploration | R Shiny / ggplot2 | ✅ Completed |
| **Tests**   | Automated Testing | Automated test suite verifying invariants (TEST-01 to TEST-07) | R testthat | ✅ 22/22 Passed |

---

## 4. Key Mathematical Formulations & Data Engineering Invariants

### 4.1 Scheduled Time Conversion & Midnight Rollover
$$\text{sched\_hour} = \lfloor \text{CRSDepTime} / 100 \rfloor, \quad \text{sched\_min} = \text{CRSDepTime} \pmod{100}$$
- **Standard Format ($0 \le \text{sched\_hour} \le 23 \land 0 \le \text{sched\_min} \le 59$):**
  $$\text{sched\_dep\_local} = \text{as.POSIXct}(\text{flight\_date}) + (\text{sched\_hour} \times 3600) + (\text{sched\_min} \times 60)$$
- **2400 Midnight Rollover ($\text{CRSDepTime} == 2400$):**
  Rolled forward to the next calendar date with `sched_dep_hour = 0`:
  $$\text{sched\_dep\_local} = \text{as.POSIXct}(\text{flight\_date}) + 86400$$

### 4.2 Deterministic Structured Keys & Idempotency
$$\text{structured\_key} = \text{flight\_date} \parallel \text{airline} \parallel \text{flight\_num} \parallel \text{origin} \parallel \text{dest} \parallel \text{sprintf}("\%04d", \text{crs\_dep})$$
$$\text{event\_id} = \text{digest}(\text{structured\_key}, \text{algo} = \text{"sha256"})$$
- Guarantees idempotent stream updates and prevents double-counting upon Kafka consumer crash recovery.

### 4.3 Mutually Exclusive Row Accounting
For every individual month $m \in \{1 \dots 12\}$ and for the entire annual dataset:
$$N_{\text{raw}, m} = N_{\text{retained}, m} + N_{\text{quarantine}, m} + N_{\text{exact\_duplicates}, m}$$

### 4.4 Non-Imputation Policy
Missing delay values for cancelled and diverted flights are **never imputed**:
$$\text{is\_valid\_arr} = (\text{cancelled} == 0 \land \text{diverted} == 0 \land \text{arr\_delay IS NOT NA})$$
$$\text{is\_valid\_dep} = (\text{cancelled} == 0 \land \text{dep\_delay IS NOT NA})$$

---

## 5. Experimental Subsets & Benchmarks

Generated by `R/generate_subsets.R` into `dataset/processed/subsets/`:

| Subset ID | Record Count | Parquet Size | CSV Size | Calendar Span | Days | Sampling Design |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `flights_100k` | 100,000 | 3.75 MB | 12.58 MB | 2025-01-01 to 2025-01-06 | 6 | Nested chronological slice (first 100K flights) |
| `flights_500k` | 500,000 | 17.88 MB | 62.79 MB | 2025-01-01 to 2025-01-29 | 29 | Nested chronological slice (first 500K flights) |
| `flights_1m` | 1,000,000 | 35.60 MB | 125.76 MB | 2025-01-01 to 2025-02-26 | 57 | Nested chronological slice (first 1M flights) |
| `flights_1m_proportional` | 1,000,000 | 37.18 MB | 125.94 MB | 2025-01-01 to 2025-12-31 | 365 | Proportional stratified monthly random sample (`seed=42`) |
| `flights_1m_equal` | 1,000,000 | 37.17 MB | 125.93 MB | 2025-01-01 to 2025-12-31 | 365 | Uniform monthly allocation (~83,333 flights/mo, `seed=42`) |

---

## 6. How to Run

### 6.1 Running the Automated Test Suite (R)
```powershell
& "C:\Program Files\R\R-4.3.2\bin\Rscript.exe" tests/testthat.R
```

### 6.2 Launching the Interactive R Shiny Dashboard
```powershell
& "C:\Program Files\R\R-4.3.2\bin\Rscript.exe" -e "shiny::runApp('shiny', port = 8080)"
```
Navigate your browser to `http://localhost:8080`.

### 6.3 Building & Running Kafka Producer / Consumer (Java)
```bash
cd java
mvn clean package -DskipTests

# Run Producer
java -cp target/airline-kafka-1.0.0.jar com.airline.kafka.FlightEventProducer

# Run Consumer
java -cp target/airline-kafka-1.0.0.jar com.airline.kafka.FlightEventConsumer
```

### 6.4 Building & Running Hadoop MapReduce Jobs
```bash
cd mr_job
mvn clean package -DskipTests

# Run Workload W1 on Hadoop / HDP cluster:
hadoop jar target/airline-mapreduce-1.0.0.jar com.airline.mr.AirlineBatchDriver w1 /airline/batch/csv /airline/results/w1
```

### 6.5 Running Hive on Tez Analytics
```bash
hive -f hive/scripts/01_create_tables.hql
hive -f hive/scripts/02_analytics_queries.hql
```

---

## 7. Automated Test Results

Executed via `testthat` on R 4.3.2:
- `TEST-01`: 2400 Midnight Rollover maps to next day 00:00:00 and sched_dep_hour = 0 (`PASS`)
- `TEST-02`: Scheduled time validation rejects invalid HHMM formats (`PASS`)
- `TEST-03`: Duplicate classification distinguishes exact duplicates from conflicting keys (`PASS`)
- `TEST-04`: Delay validity flags and zero-imputation policy are strictly maintained (`PASS`)
- `TEST-05`: Mutually exclusive row conservation equation holds (`PASS`)
- `TEST-06`: Deterministic SHA-256 event_id is reproducible and unique across flight keys (`PASS`)
- `TEST-07`: Timezone handling across US daylight saving time boundary (`PASS`)

**Summary: 7/7 Test Contexts Passed, 22/22 Assertions Passed (0 Failures, 0 Skips).**
