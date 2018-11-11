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
        options.addOption(start);
        return options;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(config, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        Options options = options();
        CommandLineParser parser = new DefaultParser();
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
        String startNumberString = commandLine.getOptionValue("start");
        if (startNumberString != null) {
            start = Long.parseUnsignedLong(startNumberString);
        }
        if (start == 0) {
            logger.error("start can not be 0");
            System.exit(1);
        }

        logger.info("input directory: " + input);
        logger.info("output directory: " + output);
        logger.info("start: " + start);

        long iter = start;
        Path inputPath = new Path(input);
        while (!finished(config, output, iter)) {
            Path lastIterationOutput = new Path(output + '/' + Long.toString(iter - 1));
            Path thisIterationOutput = new Path(output + '/' + Long.toString(iter));
            if (!iteration(config, inputPath, lastIterationOutput, thisIterationOutput, iter)) {
                throw new RuntimeException("iteration " + iter + " failed");
            }
        }
    }

    private static boolean finished(Configuration config, String output, long iteration) throws IOException {
        if (iteration == 1) return false;
        Path lastIterationOutput = new Path(output + '/' + Long.toString(iteration - 1));
        Path thisIterationOutput = new Path(output + '/' + Long.toString(iteration));
        ClusterCenterReader lastReader = new ClusterCenterReader(config);
        lastReader.add(lastIterationOutput);
        Map<Long, double[]> lastResult = lastReader.getResults();
        ClusterCenterReader thisReader = new ClusterCenterReader(config);
        thisReader.add(thisIterationOutput);
        Map<Long, double[]> thisResult = thisReader.getResults();
        return lastResult.equals(thisResult);
    }

    private static boolean iteration(Configuration config, Path input, Path lastIterationOutput, Path thisIterationOutput, long iteration) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, "k-means iteration " + Long.toString(iteration));
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleArrayWritable.class);
        job.addCacheFile(lastIterationOutput.toUri());
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, thisIterationOutput);
        return job.waitForCompletion(true);
    }
}
