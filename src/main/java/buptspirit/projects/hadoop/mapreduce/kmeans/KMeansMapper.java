package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class KMeansMapper extends Mapper<LongWritable, Text, LongWritable, MeanResult> {

    private Map<Long, double[]> centers;
    private int dimension;

    private double getDistanceSquare(double[] center, double[] sample) {
        double square = 0;
        for (int i = 0; i < dimension; i++) {
            square += (center[i] - sample[i]) * (center[i] - sample[i]);
        }
        return square;
    }

    @Override
    protected void setup(Context context) throws IOException {
        Configuration config = context.getConfiguration();
        ClusterCenterReader centerReader = new ClusterCenterReader(config);
        URI[] patternsURIs = Job.getInstance(config).getCacheFiles();
        for (URI uri: patternsURIs) {
            Path path = new Path(uri.getPath());
            centerReader.addDirectory(path);
        }
        centers = centerReader.getResults();
        dimension = centerReader.getDimension();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //value to DoubleWritable[]
        String[] values = value.toString().split("\\s+");
        double[] sample = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            sample[i] = Double.parseDouble(values[i]);
        }

        //find the nearest center
        double min = Double.MAX_VALUE;
        LongWritable cluster = new LongWritable();
        for (Map.Entry<Long, double[]> entry : centers.entrySet())
        {
            double distance = getDistanceSquare(entry.getValue(), sample);
            if (distance < min) {
                min = distance;
                cluster.set(entry.getKey());
            }
        }

        context.write(cluster, new MeanResult(1, sample));
    }
}
