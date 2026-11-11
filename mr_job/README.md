# Stage 10 — Hadoop MapReduce Batch Analytics Engine

## Overview
This module contains the Apache Hadoop MapReduce batch analytics workloads (W1, W2, W4, and Airport-Pair Delay Heatmap) for the Airline Delay Analytics project.

- **Technology**: Java 8 bytecode (`-source 1.8 -target 1.8`) | Apache Hadoop 2.7.3 / 3.x MapReduce API
- **Target Platforms**: Hortonworks Data Platform (HDP) 2.5 Sandbox / Standard Hadoop Clusters

---

## Workloads Implemented

| Workload ID | Class | Key | Additive Measures Computed |
| :--- | :--- | :--- | :--- |
| **W1** | `W1_CarrierDelayJob` | `airline` | `flight_count`, `cancelled_count`, `cancel_rate_pct`, `valid_arr_count`, `mean_arr_delay`, `mean_dep_delay`, `delayed_15_pct` |
| **W2** | `W2_AirportHourlyDelayJob` | `origin \t sched_dep_hour` | `valid_dep_flights`, `mean_dep_delay`, `delayed_15_count`, `delayed_15_pct` |
| **W4** | `W4_RouteDelayJob` | `origin-dest` | `total_flights`, `cancelled_count`, `cancel_pct`, `valid_arr_count`, `mean_arr_delay`, `mean_distance` |
| **Heatmap**| `AirportHeatmapJob` | `origin \t dest \t month` | `flight_count`, `cancel_rate`, `mean_arr_delay`, `mean_dep_delay`, `delayed_15_pct` |

---

## Building the JAR

To build the executable JAR:
```bash
cd mr_job
mvn clean package -DskipTests
```
The output JAR is generated at `target/airline-mapreduce-1.0.0.jar`.

---

## Executing on Hadoop / HDP 2.5 Cluster

Upload the cleaned dataset or experimental subsets to HDFS:
```bash
# Upload full cleaned CSV or subset to HDFS
hdfs dfs -mkdir -p /airline/batch/csv
hdfs dfs -put dataset/processed/clean_csv/*.csv /airline/batch/csv/
# Or use an experimental subset:
# hdfs dfs -put dataset/processed/subsets/flights_1m_proportional.csv /airline/batch/csv/
```

Run jobs via the unified driver `AirlineBatchDriver`:
```bash
# 1. Run Workload W1 (Carrier Delays)
hadoop jar target/airline-mapreduce-1.0.0.jar com.airline.mr.AirlineBatchDriver w1 /airline/batch/csv /airline/results/w1

# 2. Run Workload W2 (Airport Hourly Congestion)
hadoop jar target/airline-mapreduce-1.0.0.jar com.airline.mr.AirlineBatchDriver w2 /airline/batch/csv /airline/results/w2

# 3. Run Workload W4 (Route Delays)
hadoop jar target/airline-mapreduce-1.0.0.jar com.airline.mr.AirlineBatchDriver w4 /airline/batch/csv /airline/results/w4

# 4. Run Airport Heatmap Matrix
hadoop jar target/airline-mapreduce-1.0.0.jar com.airline.mr.AirlineBatchDriver heatmap /airline/batch/csv /airline/results/heatmap
```

Inspect output on HDFS:
```bash
hdfs dfs -cat /airline/results/w1/part-r-00000 | head -n 20
```
