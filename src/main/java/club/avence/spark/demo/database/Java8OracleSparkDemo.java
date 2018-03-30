package club.avence.spark.demo.database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by qian3 on 2018/3/4.
 */
public class Java8OracleSparkDemo {

    private static final String JDBC_URL = "jdbc:oracle:thin:@localhost:port:cpicdb";
    private static final String JDBC_TBALENAME = "schema.tablename";
    private static final String JDBC_USERNAME = "username";
    private static final String JDBC_PASSWORD = "password";

    public static void main(String[] args) {
        SparkSession session = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .master("local[10]")
                .getOrCreate();

        // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
        // Loading data from a JDBC source
        Dataset<Row> dataset = session.read()
                .format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", JDBC_TBALENAME)
                .option("user", JDBC_USERNAME)
                .option("password", JDBC_PASSWORD)
                .load();

        // Saving data to a JDBC source
        dataset.write()
                .format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", JDBC_TBALENAME)
                .option("user", JDBC_USERNAME)
                .option("password", JDBC_PASSWORD)
                .save();
    }
}
