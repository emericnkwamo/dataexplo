package org.exam.data.functions.filter;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;

@RequiredArgsConstructor
public class FiltFonction  implements Function<Dataset<Row>, Dataset<Row>> {

    @NonNull
    private final String sqlFilterQuery;

    @Override
    public Dataset<Row> apply(Dataset<Row> stringDataset) {

        return stringDataset.filter(sqlFilterQuery);
    }
}
