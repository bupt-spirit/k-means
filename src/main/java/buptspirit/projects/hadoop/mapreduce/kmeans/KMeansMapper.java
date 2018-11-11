package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import buptspirit.projects.hadoop.mapreduce.kmeans.DoubleArrayWritable;
/**
 * K-Means Mapper
 * Input:  key is offset of the sample in file, value is the sample text
 * Output: key is the index of the cluster, value is a the sample in DoubleArrayWritable
 */
public class KMeansMapper extends Mapper<LongWritable, Text, LongWritable, DoubleArrayWritable> {

	private double [][] centers;
	private int cluster_number;
	private int dimention;
	
	private double get_distance(double [] center, double [] sample)
	{
		double d = 0;
		for (int i = 0; i < dimention; i++)
		{
			d += (center[i] - sample[i])*(center[i] - sample[i]);
		}
		return Math.sqrt(d);
	}
	
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	//value to double []
    	String [] values = value.toString().split(" ");
    	double [] sample = new double[dimention];
    	for (int i = 0; i < dimention; i++) {
    		sample[i] = Double.parseDouble(values[i]);
    	}
    	
    	//find the nearest core
        double distance = Double.MAX_VALUE;
        int index = 0;
        for (int i = 0; i < cluster_number; i++) {
        	if (get_distance(centers[i],sample) < distance) {
        		distance = get_distance(centers[i],sample);
        		index = i;
        	}
        }
        
        //construct <k2,v2>
        LongWritable nearest_index = new LongWritable(index);
        DoubleWritable [] sample2  = new DoubleWritable[dimention];
        for (int i = 0; i < dimention; i++) {
        	sample2[i] = new DoubleWritable(sample[i]);
        }
        DoubleArrayWritable result = new DoubleArrayWritable(sample2);
        context.write(nearest_index, result);
    }

//    private static DoubleArrayWritable convert(Text text) {
//    }
}
