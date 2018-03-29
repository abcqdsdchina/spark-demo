package club.avence.spark.demo.file;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Created by qian3 on 2018/3/4.
 */
public class Java6SparkDemo {

    private static final Logger log = LoggerFactory.getLogger(Java6SparkDemo.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkDemo").setMaster("local[1000]");
        JavaSparkContext context = null;
        try {
            context = new JavaSparkContext(conf);
            JavaRDD<String> lines = context.textFile(Java6SparkDemo.class.getResource("/").getFile() + File.separator + "SparkDemo.iml");
            List<String> updatedIndexCodes = lines.map(new Function<String, String>() {
                @Override
                public String call(String line) {
                    if (StringUtils.isNotBlank(line)) {
                        String[] columns = line.split("\\|");
                        return columns[2] + "|" + columns[3];
                    }
                    return null;
                }
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String indexCode) {
                    return StringUtils.isNotBlank(indexCode);
                }
            }).distinct().collect();
            log.info("去重的指标代码数量：{}。", updatedIndexCodes.size());
        } finally {
            IOUtils.closeQuietly(context);
        }
    }
}
