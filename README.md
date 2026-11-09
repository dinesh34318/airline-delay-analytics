# Performance and Consistency of Batch and Stream Processing for Airline Delay Analytics

## A Hadoop MapReduce, Hive and Kafka Study with R Visualization

### Final-Year Big Data Research Project (Architecture & Execution Plan Version 3.1)

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
|  R 4.x: httr2 | data.table | arrow | dplyr | lubridate | digest | logger | testthat   |
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
|  - Java 8 MapReduce Jobs (W1-W4)     |  |  - Apache Kafka Broker 0.10.x           |
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
|                  R Statistical Analysis & Visualization Dashboard                 |
|            - Execution Time Scaling (W1-W4) by Dataset Size (100K-5M)             |
|            - End-to-End Latency & Throughput Distributions                        |
|            - Consistency Error Under Late/Duplicate Injections                    |
|            - Flight Delay Patterns by Carrier and Hour (ggplot2)                  |
+-----------------------------------------------------------------------------------+
```

> **Strict Architectural Rules:**
> - **R 4.x**: Exclusively for dataset collection, extraction, profiling, chunk-wise cleaning, validation, Parquet/CSV generation, subsets, and statistical visualizations.
> - **Java 8**: Exclusively for Hadoop MapReduce jobs and Kafka Producer/Consumer.
> - **HiveQL**: Exclusively for Hive analytical queries on Tez.
> - **Strict Prohibition**: Python, PySpark, Apache Spark, Spark SQL, Spark Structured Streaming, and all machine learning frameworks/libraries are strictly prohibited.

---

## 2. Project Directory Structure

```text
Airline-Delay-Analytics/
├── dataset/
│   ├── raw/
│   │   ├── monthly_zip/            # Original immutable BTS 2025 ZIP archives
│   │   └── monthly_csv/            # Extracted immutable raw CSV files
│   ├── processed/
│   │   ├── csv/                    # Cleaned CSV files (for MapReduce / HDFS)
│   │   ├── parquet/                # Cleaned Parquet files (for Hive external tables)
│   │   └── subsets/                # 100K, 500K, 1M, 5M subsets + Proportional/Equal Stratified
│   └── metadata/                   # Auditable logs, summaries, manifests & quarantine
│       ├── download_manifest.csv
│       ├── dataset_dictionary.csv
│       ├── monthly_summary.csv
│       ├── data_quality_report.csv
│       ├── quarantine_records.csv
│       ├── preprocessing_summary.json
│       └── experiment_dataset_summary.csv
├── logs/                           # Timestamped execution logs from R logger
│   ├── download.log
│   ├── extraction.log
│   ├── inspection.log
│   ├── preprocessing_log.txt
│   ├── preprocessing_memory_profile.csv
│   └── checkpoints/                # Java Kafka streaming durable checkpoints
├── R/                              # Modular R Data Engineering Scripts
│   ├── download_bts.R              # Resilient downloader with httr2, retry & SHA-256
│   ├── extract_archives.R          # Safe ZIP extractor with path traversal protection
│   ├── inspect_dataset.R           # Schema inspection, missing value profiler, dictionary
│   ├── validate_data.R             # Data quality validator and anomaly detector
│   ├── preprocess.R                # Chunked data.table cleaner, flags & SHA-256 event_id
│   ├── generate_subsets.R          # Chronological nested & stratified subset generator
│   ├── verify_parquet.R            # Arrow Parquet verification and aggregate parity check
│   └── run_pipeline.R              # Master pipeline orchestrator
├── scripts/                        # Environment discovery scripts
│   └── discover_environment.sh     # Inspects installed Hadoop, Hive, Tez, Kafka, Java
├── tests/                          # Automated Unit, Parity & Integration Tests
│   ├── testthat.R                  # testthat test runner
│   └── testthat/
│       └── test_preprocessing.R    # Tests TEST-01 through TEST-08
└── README.md                       # Comprehensive system and research documentation
```

---

## 3. Key Formulations & Architectural Specifications

### 3.1 Scheduled Time Conversion, Rollover & Timezone Semantics
$$\text{sched\_hour} = \lfloor \text{CRSDepTime} / 100 \rfloor, \quad \text{sched\_min} = \text{CRSDepTime} \pmod{100}$$
- **Normal Time:** $\text{sched\_dep\_local} = \text{as.POSIXct}(\text{flight\_date}) + (\text{sched\_hour} \times 3600) + (\text{sched\_min} \times 60)$.
- **Midnight Rollover (`2400`):** $\text{sched\_dep\_local} = \text{as.POSIXct}(\text{flight\_date}) + 86400$ (Midnight start of next day), `sched_dep_hour = 0`.
- **Corrupt Time:** $\text{sched\_min} \ge 60 \lor \text{sched\_hour} > 24 \lor \text{CRSDepTime} < 0 \to$ Quarantined.
- **Local Airport Clock vs UTC:** BTS reports times in local airport time. Workload W3 is scoped as a **local diurnal congestion analysis**. An optional UTC conversion using an airport IANA timezone mapping table evaluates synchronized windows.

### 3.2 Deterministic Structured Keys & Duplicate Classification
$$\text{structured\_key} = \text{flight\_date} \parallel \text{airline} \parallel \text{flight\_num} \parallel \text{origin} \parallel \text{dest} \parallel \text{sprintf}("\%04d", \text{crs\_dep})$$
$$\text{event\_id} = \text{digest}(\text{structured\_key}, \text{algo} = \text{"sha256"})$$
- **Exact Duplicates:** 100% field parity. Retain 1 primary instance; log duplicates to `duplicates_removed`.
- **Conflicting Duplicates:** Identical `structured_key` with differing delay or operational status are quarantined in `quarantine_records.csv` under reason `CONFLICTING_KEY_DUPLICATE`. Never silently deleted.
- **Legitimate Multi-leg Flights:** Retained because `origin` and `dest` disambiguate separate legs sharing flight numbers.

### 3.3 Mutually Exclusive Monthly & Annual Row Accounting
For every month $m$ and the entire annual dataset:
$$N_{\text{raw}, m} = N_{\text{retained}, m} + N_{\text{quarantine}, m} + N_{\text{exact\_duplicates}, m}$$

### 3.4 Zero Imputation Policy
- Missing delay values for cancelled and diverted flights are **never imputed**.
- Explicit validity flags:
  $$\text{is\_valid\_arr} = (\text{cancelled} == 0 \land \text{diverted} == 0 \land \text{arr\_delay IS NOT NA})$$
  $$\text{is\_valid\_dep} = (\text{cancelled} == 0 \land \text{dep\_delay IS NOT NA})$$

### 3.5 Java Kafka At-Least-Once Coordination
- **Delivery Guarantee:** At-Least-Once Delivery with Idempotent Aggregation.
- **Durable Checkpointing Protocol:**
  1. Pause consumption.
  2. Serialize active tumbling window buckets and durable deduplication store to `logs/checkpoints/state_ckpt_<batch_id>.tmp`.
  3. Flush and atomic rename to `state_ckpt_current.snap`.
  4. Synchronously commit offsets via `consumer.commitSync()`.
  5. Atomically publish versioned output files.
- **Crash Recovery:** Reload checkpoint, seek to committed offset (`consumer.seek()`), replayed messages filtered by durable deduplication store.

---

## 4. Software Compatibility Safeguards (HDP 2.5)

- **Hadoop Core:** Hadoop 2.7.3.
- **Hive:** Apache Hive 1.2.1 on Tez 0.7.0.
  - Parquet generation uses Parquet format 1.0 with INT96 physical timestamps or string timestamps to avoid Hive 1.2.1 timestamp decoding defects.
  - Cleaned CSV is maintained as an interchangeable primary fallback.
  - Hive 1.2.1 compatibility smoke test executed before full dataset generation.
- **Kafka:** Apache Kafka 0.10.x with Java 8 client APIs.
- **Java:** Java 8 (OpenJDK 1.8.0).
- **R:** R 4.x with `httr2`, `data.table`, `arrow`, `dplyr`, `lubridate`, `digest`, `jsonlite`, `ggplot2`, `testthat`, `logger`.
