import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by qian3 on 2018/3/4.
 */
public class Java8SparkDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkDemo").setMaster("local[1000]");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lines = context.textFile(Java8SparkDemo.class.getResource(".").getFile() + File.separator + "SparkDemo.iml");

        Set<String> updatedIndexCodes = lines.map(line -> {
            Set<String> indexCodes = new HashSet<>();
            String[] columns = line.split("\\|");
            indexCodes.add(columns[2] + "|" + columns[3]);
            return indexCodes;
        }).reduce((a, b) -> {
            a.addAll(b);
            return a;
        });
        System.out.println(updatedIndexCodes.size());
    }
}
