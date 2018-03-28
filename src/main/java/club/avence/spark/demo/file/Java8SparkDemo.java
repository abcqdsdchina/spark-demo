package club.avence.spark.demo.file;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Created by qian3 on 2018/3/4.
 */
public class Java8SparkDemo {

    private static final Logger log = LoggerFactory.getLogger(Java8SparkDemo.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkDemo").setMaster("local[1000]");
        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            JavaRDD<String> lines = context.textFile(Java8SparkDemo.class.getResource("/").getFile() + File.separator + "SparkDemo.iml");
            List<String> updatedIndexCodes = lines.map(line -> {
                String[] columns = line.split("\\|");
                return columns[2] + "|" + columns[3];
            }).distinct().collect();
            log.info("去重的年度指标串数量：{}", updatedIndexCodes.size());
        }
    }
}
