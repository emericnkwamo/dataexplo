package org.exam.data;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.exam.data.functions.filter.FiltFonction;
import org.exam.data.functions.reader.DatasetCsvReader;
import org.exam.data.functions.writer.DatasetWriter;

/**
 * Hello world!
 *
 */
@Slf4j
public class Main
{
    public static void main( String[] args )
    {

        log.info( "Starting Spark Application" );

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.name");

        SparkConf sparkConf = new SparkConf()
                .setMaster(masterUrl).setAppName(appName)
                .set("spark.executor.instances", String.valueOf(config.getInt("app.executor.nb")))
                .set("spark.executor.memory", config.getString("app.executor.memory"))
                .set("spark.executor.cores", config.getString("app.executor.cores"));

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();


        String SQLCommand = "genre_lati == 'Acacia'";
        DatasetCsvReader reader1 = new DatasetCsvReader(sparkSession, config.getString("app.data.trees.families"));
        DatasetCsvReader reader2 = new DatasetCsvReader(sparkSession, config.getString("app.data.trees.sample"));
        DatasetWriter writer = new DatasetWriter(config.getString("app.data.output"));
        FiltFonction filter = new FiltFonction(SQLCommand);

        reader1.get();
        reader2.get();
        writer.accept(
                filter.apply(
                        reader1.get()));
        log.info("OPÉRATIONS EFFECTUÉES!");

        /*SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        boolean enableSleep = config.getBoolean("app.data.sleep");
        log.info("isSleeping={} for 5min", enableSleep);
        if(enableSleep){
            try {
                Thread.sleep(1000 * 60 * 10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
    }

}
