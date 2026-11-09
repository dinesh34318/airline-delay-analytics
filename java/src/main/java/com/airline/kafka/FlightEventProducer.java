package com.airline.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stage 9 — Kafka Flight Event Producer
 *
 * Reads all clean_2025_MM.csv files from dataset/processed/clean_csv/
 * and publishes each flight record as a JSON Kafka message to topic
 * "airline.flight.events".
 *
 * Message key   : event_id (SHA-256 hex — deterministic partition routing)
 * Message value : full CSV row serialised as JSON
 *
 * Technology: Java 11+ | Apache Kafka Client 3.x
 * Run: javac + java (see compile instructions in README)
 */
public class FlightEventProducer {

    // ── Configuration ──────────────────────────────────────────────────────────
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC             = "airline.flight.events";
    private static final String CLEAN_CSV_DIR     =
        "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/dataset/processed/clean_csv";
    private static final String LOG_DIR           =
        "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/logs";
    private static final int    BATCH_SIZE        = 1000;   // flush every N msgs
    private static final long   THROTTLE_MS       = 0;      // 0 = max throughput

    // ── Logging ────────────────────────────────────────────────────────────────
    private static PrintWriter logWriter;

    private static void log(String msg) {
        String ts   = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String line = "[" + ts + "] " + msg;
        System.out.println(line);
        if (logWriter != null) { logWriter.println(line); logWriter.flush(); }
    }

    // ── CSV row → JSON ─────────────────────────────────────────────────────────
    private static String csvRowToJson(String[] headers, String[] values) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < headers.length; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(headers[i]).append("\":");
            String v = (i < values.length) ? values[i].trim() : "";
            // Numeric fields — emit without quotes
            if (isNumeric(headers[i]) && !v.isEmpty()) {
                sb.append(v);
            } else {
                sb.append("\"").append(v.replace("\"", "'")).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static final java.util.Set<String> NUMERIC_FIELDS = new java.util.HashSet<>(
        java.util.Arrays.asList(
            "flight_num","crs_dep","dep_time","dep_delay","dep_delay_15",
            "crs_arr","arr_time","arr_delay","arr_delay_15",
            "cancelled","diverted","crs_elapsed","actual_elapsed",
            "air_time","distance","carrier_delay","weather_delay",
            "nas_delay","security_delay","late_aircraft_delay","dep_time_fixed"
        )
    );

    private static boolean isNumeric(String field) {
        return NUMERIC_FIELDS.contains(field);
    }

    // ── Main ───────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {

        // Setup log file
        Files.createDirectories(Paths.get(LOG_DIR));
        logWriter = new PrintWriter(new FileWriter(LOG_DIR + "/kafka_producer.log", true));

        log("=".repeat(70));
        log("STAGE 9 — Kafka Flight Event Producer");
        log("Topic: " + TOPIC + " | Brokers: " + BOOTSTRAP_SERVERS);
        log("=".repeat(70));

        // Kafka Producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,               "all");       // strongest durability
        props.put(ProducerConfig.RETRIES_CONFIG,            3);
        props.put(ProducerConfig.LINGER_MS_CONFIG,          5);           // micro-batching
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,         65536);       // 64 KB batch
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,   "snappy");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);        // exactly-once

        AtomicLong totalSent    = new AtomicLong(0);
        AtomicLong totalFailed  = new AtomicLong(0);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Find all clean CSV files sorted by month
            File csvDir = new File(CLEAN_CSV_DIR);
            File[] csvFiles = csvDir.listFiles(
                (dir, name) -> name.startsWith("clean_2025_") && name.endsWith(".csv")
            );

            if (csvFiles == null || csvFiles.length == 0) {
                log("ERROR: No clean CSV files found in " + CLEAN_CSV_DIR);
                return;
            }

            java.util.Arrays.sort(csvFiles);  // alphabetical = chronological

            for (File csvFile : csvFiles) {
                log("Publishing: " + csvFile.getName());
                long fileSent = 0;

                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(new FileInputStream(csvFile), "UTF-8"))) {

                    String headerLine = br.readLine();
                    if (headerLine == null) continue;
                    String[] headers = headerLine.split(",", -1);

                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isBlank()) continue;
                        String[] values  = line.split(",", -1);
                        String eventId   = (values.length > 0) ? values[0].trim() : "unknown";
                        String jsonValue = csvRowToJson(headers, values);

                        ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, eventId, jsonValue);

                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                totalFailed.incrementAndGet();
                                log("SEND ERROR: " + exception.getMessage());
                            } else {
                                totalSent.incrementAndGet();
                            }
                        });

                        fileSent++;
                        if (fileSent % BATCH_SIZE == 0) {
                            producer.flush();
                        }
                        if (THROTTLE_MS > 0) Thread.sleep(THROTTLE_MS);
                    }

                    producer.flush();  // flush remaining for this file
                    log("  " + csvFile.getName() + ": " + fileSent + " events published.");
                }
            }

            producer.flush();
        }

        log("=".repeat(70));
        log("STAGE 9 PRODUCER COMPLETE");
        log("  Total events sent   : " + totalSent.get());
        log("  Total send failures : " + totalFailed.get());
        log("  Topic               : " + TOPIC);
        log("=".repeat(70));

        if (logWriter != null) logWriter.close();
    }
}
