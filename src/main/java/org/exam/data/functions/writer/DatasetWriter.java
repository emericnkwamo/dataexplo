package org.exam.data.functions.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class DatasetWriter implements Consumer<Dataset<Row>> {

    private final String parquetOutputPathStr;

    @Override
    public void accept(Dataset<Row> rowDataset) {
        Dataset<Row> ds = rowDataset.cache();
        log.info("ecriture des données dans le lien {}", ds.count(), parquetOutputPathStr);
        ds.printSchema();
        ds.show(5);
        ds.coalesce(1)
                .write().mode(SaveMode.Overwrite)
                .option("header", true)
                .format("csv")
                .save(parquetOutputPathStr);
        ds.unpersist();
    }
}