package com.airline.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * Stage 10 — Main Unified Driver for Hadoop MapReduce Batch Jobs
 *
 * Usage:
 *   hadoop jar airline-mapreduce-1.0.0.jar com.airline.mr.AirlineBatchDriver <job_type> <input_path> <output_path>
 *
 * Supported job types:
 *   - w1       : W1 Carrier Delay Analytics
 *   - w2       : W2 Origin Airport Hourly Congestion
 *   - w4       : W4 Route Delays & Distance Metrics
 *   - heatmap  : Airport-Pair Heatmap Matrix
 */
public class AirlineBatchDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            printUsage();
            System.exit(1);
        }

        String jobType = args[0].trim().toLowerCase();
        String[] jobArgs = Arrays.copyOfRange(args, 1, args.length);
        Configuration conf = new Configuration();

        int exitCode;
        switch (jobType) {
            case "w1":
            case "carrier":
                System.out.println("Starting Workload W1: Carrier Delay Analytics...");
                exitCode = ToolRunner.run(conf, new W1_CarrierDelayJob(), jobArgs);
                break;

            case "w2":
            case "hourly":
                System.out.println("Starting Workload W2: Origin Airport Hourly Congestion...");
                exitCode = ToolRunner.run(conf, new W2_AirportHourlyDelayJob(), jobArgs);
                break;

            case "w4":
            case "route":
                System.out.println("Starting Workload W4: Route Delays & Distance Analytics...");
                exitCode = ToolRunner.run(conf, new W4_RouteDelayJob(), jobArgs);
                break;

            case "heatmap":
                System.out.println("Starting Airport Heatmap Matrix Job...");
                exitCode = ToolRunner.run(conf, new AirportHeatmapJob(), jobArgs);
                break;

            default:
                System.err.println("ERROR: Unknown job type '" + jobType + "'");
                printUsage();
                exitCode = 1;
        }

        System.exit(exitCode);
    }

    private static void printUsage() {
        System.err.println("===============================================================================");
        System.err.println("AIRLINE DELAY ANALYTICS — HADOOP MAPREDUCE BATCH DRIVER (STAGE 10)");
        System.err.println("===============================================================================");
        System.err.println("Usage:");
        System.err.println("  hadoop jar airline-mapreduce-1.0.0.jar com.airline.mr.AirlineBatchDriver <job_type> <input_path> <output_path>");
        System.err.println();
        System.err.println("Available Job Types:");
        System.err.println("  w1       : Workload W1 - Carrier volume, cancellations, mean arrival delay, 15+ min %");
        System.err.println("  w2       : Workload W2 - Origin airport hourly departure congestion profile");
        System.err.println("  w4       : Workload W4 - Route-level flight volume, delay, and distance metrics");
        System.err.println("  heatmap  : Airport-to-airport monthly delay and volume matrix for heatmaps");
        System.err.println();
        System.err.println("Example:");
        System.err.println("  hadoop jar airline-mapreduce-1.0.0.jar com.airline.mr.AirlineBatchDriver w1 /data/airline_delay/csv /data/airline_delay/output_w1");
        System.err.println("===============================================================================");
    }
}
