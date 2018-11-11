package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable {

    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    public DoubleArrayWritable(DoubleWritable[] values) {
        super(DoubleWritable.class, values);
    }

    @Override
    public String toString() {
        DoubleWritable[] writables = (DoubleWritable[])get();
        StringBuilder stringBuilder = new StringBuilder();
        for (DoubleWritable writable: writables) {
            stringBuilder.append(writable.get());
            stringBuilder.append(' ');
        }
        return stringBuilder.toString();
    }
}
