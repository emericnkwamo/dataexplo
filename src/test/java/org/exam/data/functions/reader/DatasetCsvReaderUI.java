package org.exam.data.functions.reader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

@Slf4j
public class DatasetCsvReaderUI {

    private static final Config testConf = ConfigFactory.load("application.conf");

    private static SparkSession sparkSess;
    @BeforeClass
    public static void setUp(){
        log.info("initialisation de la sparkSession ...");
        sparkSess = SparkSession.builder()
                .master(testConf.getString("app.master"))
                .appName(testConf.getString("app.name"))
                .getOrCreate();
    }
    @Test
    public void testReader(){
        log.info("test sur le reader ...");
        String testInputPath = testConf.getString("app.data.trees.families");
        DatasetCsvReader reader = new DatasetCsvReader(
                sparkSess, testInputPath
        );
        Dataset<Row> ds = reader.get().cache();
        ds.show(5,false);
    }
}
