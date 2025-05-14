package org.example.spark.transformationBeans;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.spark.dto.TransformationInputDto;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class Filter implements Transformation {
    public Dataset<Row> apply(TransformationInputDto transformationInputDto, List<Dataset<Row>> datasetList){
        Dataset<Row> df = datasetList.get(0);
        log.info("filter called with expression :: {}", transformationInputDto.getFilterExpr());
        traceColumn(transformationInputDto);
        return df.filter(transformationInputDto.getFilterExpr());
    }
    public void traceColumn(TransformationInputDto transformationInputDto){
        log.info("traceColumn not applicable for filter as no column transformation applied..");
    }
}
