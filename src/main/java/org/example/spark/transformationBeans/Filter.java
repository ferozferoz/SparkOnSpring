package org.example.spark.transformationBeans;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.spark.dto.TransformationDto;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class Filter implements Transformation {
    public Dataset<Row> apply(TransformationDto transformationDto, List<Dataset<Row>> datasetList){
        Dataset<Row> df = datasetList.get(0);
        log.info("filter called with expression :: {}",transformationDto.getFilterExpr());
        return df.filter(transformationDto.getFilterExpr());
    }
}
