package org.example.spark.transformationBeans;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.spark.dto.TransformationInputDto;
import java.util.List;

public interface Transformation {

    Dataset<Row> apply(TransformationInputDto transformationInputDto, List<Dataset<Row>> datasetList);
    void traceColumn(TransformationInputDto transformationInputDto);
}
