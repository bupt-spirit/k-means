package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * K-Means Reducer
 * Input:  key is the index of the cluster, value is the partial results
 * Output: key is the index of the cluster, value is the new center (the mean of all partial result)
 */
public class KMeansReducer extends Reducer<LongWritable, PartialResult, LongWritable, DoubleArrayWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<PartialResult> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
