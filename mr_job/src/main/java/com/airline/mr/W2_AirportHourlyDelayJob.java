package com.airline.mr;

import com.airline.mr.model.FlightRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Stage 10 — Workload W2: Departure Delay by Origin Airport and Scheduled Hour
 *
 * Keys: origin (IATA airport code) + "\t" + sched_dep_hour (0..23)
 * Measures:
 *   - valid_dep_flights
 *   - sum_dep_delay
 *   - mean_dep_delay
 *   - delayed_15_count
 *   - delayed_15_pct
 */
public class W2_AirportHourlyDelayJob extends Configured implements Tool {

    public static class AirportHourMapper extends Mapper<LongWritable, Text, Text, FlightRecordWritable> {

        private final Text outKey = new Text();
        private final FlightRecordWritable outVal = new FlightRecordWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("event_id")) {
                return;
            }

            String[] tokens = line.split(",", -1);
            if (tokens.length < 28) {
                return;
            }

            String origin = tokens[5].trim();
            if (origin.isEmpty()) {
                return;
            }

            // crs_dep or dep_time_fixed: HHMM
            int crsDep = parseInt(tokens[7]);
            int hour = (crsDep == 2400) ? 0 : (crsDep / 100);
            if (hour < 0 || hour > 23) {
                return; // invalid hour
            }

            long cancelled = parseLong(tokens[15]);
            double depDelay = parseDouble(tokens[9]);
            long validDep = (cancelled == 0L && !tokens[9].trim().isEmpty()) ? 1L : 0L;
            double effectiveDepDelay = (validDep == 1L) ? depDelay : 0.0;
            long delayed15 = (validDep == 1L && depDelay >= 15.0) ? 1L : 0L;

            outKey.set(origin + "\t" + String.format("%02d", hour));
            outVal.set(1L, cancelled, 0L, 0.0, validDep, effectiveDepDelay, delayed15, 0.0);
            context.write(outKey, outVal);
        }

        private static int parseInt(String s) {
            if (s == null || s.trim().isEmpty()) return -1;
            try { return (int) Double.parseDouble(s.trim()); } catch (NumberFormatException e) { return -1; }
        }

        private static double parseDouble(String s) {
            if (s == null || s.trim().isEmpty()) return 0.0;
            try { return Double.parseDouble(s.trim()); } catch (NumberFormatException e) { return 0.0; }
        }

        private static long parseLong(String s) {
            if (s == null || s.trim().isEmpty()) return 0L;
            try { return (long) Double.parseDouble(s.trim()); } catch (NumberFormatException e) { return 0L; }
        }
    }

    public static class AirportHourCombiner extends Reducer<Text, FlightRecordWritable, Text, FlightRecordWritable> {
        private final FlightRecordWritable result = new FlightRecordWritable();

        @Override
        protected void reduce(Text key, Iterable<FlightRecordWritable> values, Context context)
                throws IOException, InterruptedException {
            result.clear();
            for (FlightRecordWritable val : values) {
                result.merge(val);
            }
            context.write(key, result);
        }
    }

    public static class AirportHourReducer extends Reducer<Text, FlightRecordWritable, Text, Text> {
        private final FlightRecordWritable agg = new FlightRecordWritable();
        private final Text resultText = new Text();

        @Override
        protected void reduce(Text key, Iterable<FlightRecordWritable> values, Context context)
                throws IOException, InterruptedException {
            agg.clear();
            for (FlightRecordWritable val : values) {
                agg.merge(val);
            }

            // Output format:
            // origin \t hour \t departures \t mean_dep_delay \t delay15_count \t delay15_pct
            String line = String.format("%d\t%.4f\t%d\t%.4f",
                    agg.getValidDepCount(),
                    agg.getMeanDepDelay(),
                    agg.getDelayed15Plus(),
                    agg.getDelayed15Pct());

            resultText.set(line);
            context.write(key, resultText);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: W2_AirportHourlyDelayJob <input_path> <output_path>");
            return 1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Airline Delay - W2 Origin Airport Hourly Congestion");
        job.setJarByClass(W2_AirportHourlyDelayJob.class);

        job.setMapperClass(AirportHourMapper.class);
        job.setCombinerClass(AirportHourCombiner.class);
        job.setReducerClass(AirportHourReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlightRecordWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new W2_AirportHourlyDelayJob(), args);
        System.exit(exitCode);
    }
}
