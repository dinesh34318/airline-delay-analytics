package com.airline.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Stage 9 — Kafka Flight Event Consumer
 *
 * Subscribes to topic "airline.flight.events" and processes each
 * flight record JSON message. Computes real-time aggregations:
 *   - Per-carrier running delay average
 *   - Cancellation counter
 *   - 15-min delay rate
 *
 * Outputs a rolling summary log every 100K messages consumed.
 *
 * Technology: Java 11+ | Apache Kafka Client 3.x
 */
public class FlightEventConsumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC             = "airline.flight.events";
    private static final String GROUP_ID          = "airline-delay-analytics-group";
    private static final String LOG_DIR           =
        "C:/Users/DELL/.gemini/antigravity/scratch/Airline-Delay-Analytics/logs";
    private static final long   POLL_TIMEOUT_MS   = 1000;
    private static final long   LOG_INTERVAL      = 100_000;

    private static PrintWriter logWriter;

    private static void log(String msg) {
        String ts   = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String line = "[" + ts + "] " + msg;
        System.out.println(line);
        if (logWriter != null) { logWriter.println(line); logWriter.flush(); }
    }

    // ── Simple JSON field extractor ────────────────────────────────────────────
    private static String extractField(String json, String field) {
        String key = "\"" + field + "\":";
        int idx = json.indexOf(key);
        if (idx < 0) return "";
        int start = idx + key.length();
        // Skip quote if string value
        boolean quoted = json.charAt(start) == '"';
        if (quoted) start++;
        int end = quoted
            ? json.indexOf('"', start)
            : json.indexOf(',', start);
        if (end < 0) end = json.indexOf('}', start);
        if (end < 0 || end <= start) return "";
        return json.substring(start, end).trim();
    }

    // ── Running aggregation state ──────────────────────────────────────────────
    private static final Map<String, Long>   carrierFlights   = new HashMap<>();
    private static final Map<String, Double> carrierDelaySum  = new HashMap<>();
    private static final Map<String, Long>   carrierCancelled = new HashMap<>();
    private static       long totalConsumed = 0;
    private static       long totalDelayed  = 0;  // dep_delay >= 15

    private static void processRecord(String json) {
        String airline   = extractField(json, "airline");
        String depDelay  = extractField(json, "dep_delay");
        String cancelled = extractField(json, "cancelled");

        if (airline == null || airline.isEmpty()) return;

        carrierFlights.merge(airline, 1L, Long::sum);

        try {
            double dd = Double.parseDouble(depDelay);
            carrierDelaySum.merge(airline, dd, Double::sum);
            if (dd >= 15) totalDelayed++;
        } catch (NumberFormatException ignored) {}

        try {
            int c = Integer.parseInt(cancelled);
            if (c == 1) carrierCancelled.merge(airline, 1L, Long::sum);
        } catch (NumberFormatException ignored) {}

        totalConsumed++;
    }

    private static void printSummary() {
        log("--- Streaming Summary @ " + totalConsumed + " records consumed ---");
        log(String.format("  Total consumed     : %,d", totalConsumed));
        log(String.format("  Delayed 15+ min    : %,d (%.2f%%)",
            totalDelayed, totalConsumed > 0 ? 100.0 * totalDelayed / totalConsumed : 0));

        // Top 5 carriers by delay
        carrierDelaySum.entrySet().stream()
            .sorted((a, b) -> Double.compare(
                b.getValue() / carrierFlights.getOrDefault(b.getKey(), 1L),
                a.getValue() / carrierFlights.getOrDefault(a.getKey(), 1L)
            ))
            .limit(5)
            .forEach(e -> {
                long n = carrierFlights.getOrDefault(e.getKey(), 1L);
                log(String.format("  Carrier %-5s | Flights=%,d | AvgDepDelay=%.2f min | Cancelled=%,d",
                    e.getKey(), n, e.getValue() / n,
                    carrierCancelled.getOrDefault(e.getKey(), 0L)));
            });
        log("------------------------------------------------------------------");
    }

    // ── Main ───────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {

        Files.createDirectories(Paths.get(LOG_DIR));
        logWriter = new PrintWriter(new FileWriter(LOG_DIR + "/kafka_consumer.log", true));

        log("=".repeat(70));
        log("STAGE 9 — Kafka Flight Event Consumer");
        log("Topic: " + TOPIC + " | Group: " + GROUP_ID);
        log("=".repeat(70));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);       // manual commit
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   500);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,    5_242_880);   // 5 MB

        // Graceful shutdown hook
        final boolean[] running = {true};
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log("Shutdown signal received. Stopping consumer...");
            running[0] = false;
        }));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            log("Subscribed to topic: " + TOPIC + ". Polling...");

            while (running[0]) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record.value());
                    if (totalConsumed % LOG_INTERVAL == 0 && totalConsumed > 0) {
                        printSummary();
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();  // commit after full batch
                }
            }
        }

        // Final summary
        printSummary();
        log("STAGE 9 CONSUMER STOPPED. Total consumed: " + totalConsumed);
        if (logWriter != null) logWriter.close();
    }
}
