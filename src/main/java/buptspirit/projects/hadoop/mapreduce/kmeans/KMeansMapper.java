package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * K-Means Mapper
 * Input:  key is offset of the sample in file, value is the sample text
 * Output: key is the index of the cluster, value is a the sample in DoubleArrayWritable
 */
public class KMeansMapper extends Mapper<LongWritable, Text, LongWritable, DoubleArrayWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

//    private static DoubleArrayWritable convert(Text text) {
//    }
}
