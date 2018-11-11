package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.DoubleWritable;

import java.util.ArrayList;

public class KMeans {

    public static void main(String[] args) {
        ArrayList<DoubleWritable> doubleWritables = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            DoubleWritable doubleWritable = new DoubleWritable((double)i);
            doubleWritables.add(doubleWritable);
        }
        DoubleWritable[] inArray = new DoubleWritable[doubleWritables.size()];
        inArray = doubleWritables.toArray(inArray);
        DoubleArrayWritable doubleArrayWritable = new DoubleArrayWritable(inArray);
        String test = doubleArrayWritable.toString();
    }

    public static void iteration() {
    }
}
