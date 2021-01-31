package org.exam.data.functions.filter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.exam.data.Record;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class filtFonctionUI {

    private static final Config testConf = ConfigFactory.load("application.conf");
    private static SparkSession sparkSession;

    @BeforeClass
    public static void setUp() {
        log.info("initializing sparkSession ...");
        sparkSession = SparkSession.builder()
                .master(testConf.getString("app.master"))
                .appName(testConf.getString("app.name"))
                .getOrCreate();
    }

    @Test
    public void filterSQLTest() {

        Dataset<Record> entry = sparkSession.createDataset(
                Arrays.asList(new Record("alcatia", "Fabaceae"),
                        new Record("acer", "Sapindaceae"),
                        new Record("aescukus", "Sapindaceae"))
                , Encoders.bean(Record.class)
        );

        Dataset<Row> dats = entry.toDF();
        dats.show(false);
        String sqlFilterQuery = "genre_lati == 'alcatia'";

        log.info("Application du filtre");
        FiltFonction datasetSQLFilter = new FiltFonction(sqlFilterQuery);
        Dataset<Row> dsResponse = datasetSQLFilter.apply(dats);

        log.info("affichage du resultat");
        dsResponse.show(false);
        long a = dsResponse.count();
        log.info("{}", a);

        assertThat(dsResponse.count() == 1).isTrue();
    }
}
