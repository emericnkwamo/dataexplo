package org.exam.data.functions.reader;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.function.Supplier;


@Slf4j
@RequiredArgsConstructor
public class DatasetCsvReader implements Supplier<Dataset<Row>> {
    @NonNull
    private final SparkSession sparkSession;
    @NonNull
    private final String csvInputPath;

    @Override
    public Dataset<Row> get() {
        log.info("lecture des données sur le chemin = {}", csvInputPath);
        boolean hasValidInput = csvInputPath != null &&
                !csvInputPath.isEmpty() &&
                !sparkSession.sparkContext().isStopped();

        return sparkSession.read()
                .option("header",true)
                .option("delimiter",";")
                .csv(csvInputPath);

    }
}
