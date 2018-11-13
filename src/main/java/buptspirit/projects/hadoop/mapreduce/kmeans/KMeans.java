package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class KMeans {

    private static Logger logger = Logger.getLogger(KMeans.class);

    private static Options options() {
        Options options = new Options();
        Option input = new Option("i", "input", true, "input directory path");
        input.setRequired(true);
        options.addOption(input);
        Option output = new Option("o", "output", true, "output directory path");
        output.setRequired(true);
        options.addOption(output);
        Option start = new Option("s", "start", true, "initial iteration number");
        start.setRequired(false);
        Option maxIterations = new Option("m", "max-iterations", true, "max iterations number");
        start.setRequired(false);
        options.addOption(maxIterations);
        Option threshold = new Option("t", "threshold", true, "threshold of cluster center changing");
        start.setRequired(false);
        options.addOption(threshold);
        options.addOption(start);
        return options;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(config, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        Options options = options();
        CommandLineParser parser = new PosixParser();
        HelpFormatter helpFormatter = new HelpFormatter();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, remainingArgs);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            helpFormatter.printHelp("k-means", options);
            System.exit(1);
        }

        String input = commandLine.getOptionValue("input");
        String output = commandLine.getOptionValue("output");
        long start = 1;
        long maxIterations = Long.MAX_VALUE;
        double threshold = 0;
        String startNumberString = commandLine.getOptionValue("start");
        if (startNumberString != null) {
            start = Long.parseUnsignedLong(startNumberString);
        }
        String maxIterationsString = commandLine.getOptionValue("max-iterations");
        if (maxIterationsString != null) {
            maxIterations = Long.parseUnsignedLong(maxIterationsString);
        }
        String thresholdString = commandLine.getOptionValue("threshold");
        if (thresholdString != null) {
            threshold = Double.parseDouble(thresholdString);
        }
        if (start == 0) {
            logger.error("start can not be 0");
            System.exit(1);
        }
        if (threshold < 0) {
            logger.error("threshold can not be less then 0");
            System.exit(1);
        }

        logger.info("input directory: " + input);
        logger.info("output directory: " + output);
        logger.info("start: " + start);

        long iter = start;
        Path inputPath = new Path(input);
        while (!finished(config, output, iter, threshold) && iter <= maxIterations) {
            Path lastIterationOutput = new Path(output + '/' + Long.toString(iter - 1));
            Path thisIterationOutput = new Path(output + '/' + Long.toString(iter));
            logger.info("about to start new iteration " + iter);
            logger.info("lastIterationOutput: " + lastIterationOutput.toString());
            logger.info("thisIterationOutput: " + thisIterationOutput.toString());
            if (!iteration(config, inputPath, lastIterationOutput, thisIterationOutput, iter)) {
                throw new RuntimeException("iteration " + iter + " failed");
            }
            ++iter;
        }
    }

    private static boolean finished(Configuration config, String output, long iteration, double threshold) throws IOException {
        if (iteration == 1) return false;
        Path lastIterationOutput = new Path(output + '/' + Long.toString(iteration - 2));
        Path thisIterationOutput = new Path(output + '/' + Long.toString(iteration - 1));
        ClusterCenterReader lastReader = new ClusterCenterReader(config);
        lastReader.addDirectory(lastIterationOutput);
        Map<Long, double[]> lastResult = lastReader.getResults();
        ClusterCenterReader thisReader = new ClusterCenterReader(config);
        thisReader.addDirectory(thisIterationOutput);
        Map<Long, double[]> thisResult = thisReader.getResults();
        return isSameCenter(lastResult, thisResult, threshold);
    }

    private static boolean iteration(
            Configuration config,
            Path input,
            Path lastIterationOutput,
            Path thisIterationOutput,
            long iteration)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, "k-means iteration " + Long.toString(iteration));
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansReducer.class);
        job.setReducerClass(KMeansReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MeanResult.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(MeanResult.class);
        job.addCacheFile(lastIterationOutput.toUri());
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, thisIterationOutput);
        return job.waitForCompletion(true);
    }

    private static boolean isSameCenter(Map<Long, double[]> map1, Map<Long, double[]> map2, double threshold) {
        for (Map.Entry<Long, double[]> entry: map1.entrySet()) {
            if (!map2.containsKey(entry.getKey())) {
                return false;
            } else {
                double[] center1 = entry.getValue();
                double[] center2 = map2.get(entry.getKey());
                assert center1.length == center2.length;
                for (int i = 0; i < center1.length; ++i) {
                    if (Math.abs(center1[i] - center2[i]) > threshold)
                        return false;
                }
            }

        }
        return true;
    }
}
