package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class ClusterCenterReader {

    private Map<Long, double[]> results = new HashMap<>();
    private Configuration config;
    private int dimensions;

    public ClusterCenterReader(Configuration config) {
        this.config = config;
        this.dimensions = -1;
    }

    public void add(Path path) throws IOException {
        FileSystem fs = path.getFileSystem(config);
        FSDataInputStream inputStream = fs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        reader.lines().forEach(line -> {
            String[] splited = line.split(" ");
            if (dimensions == -1) {
                dimensions = splited.length - 1;
            } else if (dimensions != splited.length - 1) {
                throw new RuntimeException("dimentsions of cluster center is not match");
            }
            long clusterKey = Long.parseUnsignedLong(splited[0]);
            double[] center = new double[dimensions];
            for (int i = 1; i < splited.length; ++i) {
                center[i - 1] = Double.parseDouble(splited[i]);
            }
            results.put(clusterKey, center);
        });
    }

    public int getDimensions() {
        return dimensions;
    }

    public Map<Long, double[]> getResults() {
        return results;
    }
}
