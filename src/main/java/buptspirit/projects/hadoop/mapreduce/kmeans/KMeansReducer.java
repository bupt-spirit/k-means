package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * K-Means Reducer
 * Input:  key is the index of the cluster, value is the partial results
 * Output: key is the index of the cluster, value is the new center (the mean of all partial result)
 */
public class KMeansReducer extends Reducer<LongWritable, MeanResult, LongWritable, MeanResult> {

    @Override
    protected void reduce(LongWritable key, Iterable<MeanResult> values, Context context) throws IOException, InterruptedException {

        int dimension = -1;
        long count = 0;
        double[] sum = null;
        for (MeanResult meanResult : values) {
            if (dimension == -1) {
                dimension = meanResult.getSum().length;
                sum = new double[dimension];
            }
            count += meanResult.getCount();
            double[] partialSum = meanResult.getSum();
            for (int i = 0; i < dimension; ++i) {
                sum[i] += partialSum[i];
            }
        }
        assert sum != null; // never happens
        MeanResult meanResult = new MeanResult(count, sum);
        context.write(key, meanResult);
    }
}
