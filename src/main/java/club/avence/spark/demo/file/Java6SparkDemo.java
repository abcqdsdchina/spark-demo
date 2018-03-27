package club.avence.spark.demo.file;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by qian3 on 2018/3/4.
 */
public class Java6SparkDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkDemo").setMaster("local[1000]");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lines = context.textFile(Java6SparkDemo.class.getResource(".").getFile() + File.separator + "SparkDemo.iml");

        Set<String> updatedIndexCodes = lines.map(new Function<String, Set<String>>() {
            @Override
            public Set<String> call(String line) {
                Set<String> indexCodes = new HashSet<>();
                if (StringUtils.isNotBlank(line)) {
                    String[] columns = line.split("\\|");
                    indexCodes.add(columns[2] + "|" + columns[3]);
                }
                return indexCodes;
            }
        }).reduce(new Function2<Set<String>, Set<String>, Set<String>>() {
            @Override
            public Set<String> call(Set<String> a, Set<String> b) {
                a.addAll(b);
                return a;
            }
        });
        System.out.println(updatedIndexCodes.size());
    }
}
