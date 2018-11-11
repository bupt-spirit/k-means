package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.DoubleWritable;
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
    	
    	

    	int dimention = -1;
    	int count = 0;
    	double [] sum = null;
    	for (PartialResult result : values)
    	{
    		if (dimention == -1) {
    			dimention = result.getResult().toStrings().length;
    			sum = new double[dimention];
    			//FIX LATER
    		}
    		
    		count += result.getCount().get();
    		DoubleArrayWritable partial = result.getResult();
    		
    		
    	}
    }
}
