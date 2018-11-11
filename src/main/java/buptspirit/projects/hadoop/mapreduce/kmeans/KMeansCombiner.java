package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
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
        int dimension = values.iterator().next().get().length;
        DoubleWritable[] sum = new DoubleWritable[dimension];
        int count = 0;
        for(DoubleArrayWritable array: values){
            String[] vals = array.toString().split(" ");
            for(int i=0; i<dimension; i++){
                sum[i] = new DoubleWritable(Double.valueOf(sum[i].toString())+Double.valueOf(vals[i]));
            }
            count+=1;
        }

        DoubleArrayWritable result = new DoubleArrayWritable();
        result.set(sum);

        PartialResult partialResult = new PartialResult(key, result);
    }
}
