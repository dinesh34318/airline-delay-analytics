#!/usr/bin/env bash
# ==============================================================================
# scripts/discover_environment.sh
# Purpose: Inspect and verify actual installed versions on Hortonworks Sandbox 2.5
# ==============================================================================

set -euo pipefail

echo "=========================================================="
echo " Hortonworks Data Platform (HDP) Environment Discovery"
echo "=========================================================="

echo -e "\n[1] Java Runtime Environment:"
java -version 2>&1 | head -n 3 || echo "Java not found in PATH"

echo -e "\n[2] Hadoop Core Version:"
hadoop version 2>&1 | head -n 2 || echo "Hadoop not found in PATH"

echo -e "\n[3] Apache Hive Version:"
hive --version 2>&1 | head -n 2 || echo "Hive not found in PATH"

echo -e "\n[4] Apache Tez Client Version / Jars:"
find /usr/hdp/current/tez-client/lib -name "tez-api-*.jar" 2>/dev/null || echo "Tez client jar search complete"

echo -e "\n[5] Apache Kafka Broker Version / Jars:"
find /usr/hdp/current/kafka-broker/libs -name "kafka_*.jar" 2>/dev/null || echo "Kafka broker jar search complete"

echo -e "\n[6] HDFS Cluster Health:"
hdfs dfsadmin -report 2>&1 | head -n 10 || echo "HDFS report not available"

echo -e "\n=========================================================="
echo " Discovery Completed Successfully"
echo "=========================================================="
