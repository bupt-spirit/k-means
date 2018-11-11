package buptspirit.projects.hadoop.mapreduce.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

class ClusterCenterReader {

    private Map<Long, double[]> results = new HashMap<>();
    private Configuration config;
    private int dimension;

    public ClusterCenterReader(Configuration config) {
        this.config = config;
        this.dimension = -1;
    }

    public void addDirectory(Path path) throws IOException {
        FileSystem fs = path.getFileSystem(config);
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, true);
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            FSDataInputStream inputStream = fs.open(fileStatus.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            reader.lines().forEach(line -> {
                String[] splited = line.split("\\s+");
                if (dimension == -1) {
                    dimension = splited.length - 1;
                } else if (dimension != splited.length - 1) {
                    throw new RuntimeException("dimentsions of cluster center is not match");
                }
                long clusterKey = Long.parseUnsignedLong(splited[0]);
                double[] center = new double[dimension];
                for (int i = 1; i < splited.length; ++i) {
                    center[i - 1] = Double.parseDouble(splited[i]);
                }
                results.put(clusterKey, center);
            });
        }
    }

    public int getDimension() {
        return dimension;
    }

    public Map<Long, double[]> getResults() {
        return results;
    }
}
