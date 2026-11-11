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
 * Stage 10 — Airport Heatmap Analytics Job
 *
 * Computes monthly origin-to-destination matrix for heatmap visualization:
 * Key: origin \t dest \t month
 * Value: flight count, cancellations, mean arrival delay, mean departure delay
 */
public class AirportHeatmapJob extends Configured implements Tool {

    public static class HeatmapMapper extends Mapper<LongWritable, Text, Text, FlightRecordWritable> {

        private final Text outKey = new Text();
        private final FlightRecordWritable outVal = new FlightRecordWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("event_id")) {
                return;
            }

            String[] tokens = line.split(",", -1);
            if (tokens.length < 22) {
                return;
            }

            String date   = tokens[1].trim(); // YYYY-MM-DD
            String origin = tokens[5].trim();
            String dest   = tokens[6].trim();
            if (origin.isEmpty() || dest.isEmpty() || date.length() < 7) {
                return;
            }

            String month = date.substring(5, 7);

            long cancelled = parseLong(tokens[15]);
            long diverted  = parseLong(tokens[17]);

            double arrDelay = parseDouble(tokens[13]);
            long validArr = (cancelled == 0L && diverted == 0L && !tokens[13].trim().isEmpty()) ? 1L : 0L;
            double effectiveArrDelay = (validArr == 1L) ? arrDelay : 0.0;

            double depDelay = parseDouble(tokens[9]);
            long validDep = (cancelled == 0L && !tokens[9].trim().isEmpty()) ? 1L : 0L;
            double effectiveDepDelay = (validDep == 1L) ? depDelay : 0.0;
            long delayed15 = (validDep == 1L && depDelay >= 15.0) ? 1L : 0L;

            double distance = parseDouble(tokens[21]);

            outKey.set(origin + "\t" + dest + "\t" + month);
            outVal.set(1L, cancelled, validArr, effectiveArrDelay, validDep, effectiveDepDelay, delayed15, distance);
            context.write(outKey, outVal);
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

    public static class HeatmapCombiner extends Reducer<Text, FlightRecordWritable, Text, FlightRecordWritable> {
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

    public static class HeatmapReducer extends Reducer<Text, FlightRecordWritable, Text, Text> {
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
            // origin \t dest \t month \t flights \t cancel_rate \t mean_arr_delay \t mean_dep_delay \t delay15_pct
            String line = String.format("%d\t%.4f\t%.4f\t%.4f\t%.4f",
                    agg.getFlightCount(),
                    agg.getCancelRatePct(),
                    agg.getMeanArrDelay(),
                    agg.getMeanDepDelay(),
                    agg.getDelayed15Pct());

            resultText.set(line);
            context.write(key, resultText);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: AirportHeatmapJob <input_path> <output_path>");
            return 1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Airline Delay - Airport Heatmap Matrix");
        job.setJarByClass(AirportHeatmapJob.class);

        job.setMapperClass(HeatmapMapper.class);
        job.setCombinerClass(HeatmapCombiner.class);
        job.setReducerClass(HeatmapReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlightRecordWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new AirportHeatmapJob(), args);
        System.exit(exitCode);
    }
}
