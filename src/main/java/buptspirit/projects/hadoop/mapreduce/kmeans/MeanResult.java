package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

class MeanResult implements Writable {

    private long count;
    private double[] sum;

    public MeanResult() {}

    public MeanResult(long count, double[] sum) {
        this.count = count;
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double[] getSum() {
        return sum;
    }

    public void setSum(double[] sum) {
        this.sum = sum;
    }

    public double[] getResult() {
        return Arrays.stream(sum).map(v -> v / count).toArray();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(count);
        dataOutput.writeInt(sum.length);
        for (double d: sum) {
            dataOutput.writeDouble(d);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readLong();
        int dimension = dataInput.readInt();
        sum = new double[dimension];
        for (int i = 0; i < dimension; ++i) {
            sum[i] = dataInput.readDouble();
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        double[] res = getResult();
        for (double value: res) {
            builder.append(value);
            builder.append(' ');
        }
        return builder.toString();
    }
}
