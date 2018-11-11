package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class PartialResult implements Writable {

    private LongWritable count;
    private DoubleArrayWritable result;

    public PartialResult(LongWritable count, DoubleArrayWritable result) {
        this.count = count;
        this.result = result;
    }

    public LongWritable getCount() {
        return count;
    }

    public void setCount(LongWritable count) {
        this.count = count;
    }

    public DoubleArrayWritable getResult() {
        return result;
    }

    public void setResult(DoubleArrayWritable result) {
        this.result = result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        count.write(dataOutput);
        result.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count.readFields(dataInput);
        result.readFields(dataInput);
    }
}
