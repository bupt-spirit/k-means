package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * K-Means Combiner
 * Input:  key is the index of the cluster, value is the list of the samples assigned to the same cluster
 * Output: key is the index of the cluster, value is a partial result
 */
public class KMeansCombiner extends Reducer<LongWritable, DoubleArrayWritable, LongWritable, PartialResult> {

    @Override
    protected void reduce(LongWritable key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
