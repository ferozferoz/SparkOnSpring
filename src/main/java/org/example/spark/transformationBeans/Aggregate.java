package org.example.spark.transformationBeans;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.spark.dto.OperationAndColumn;
import org.example.spark.dto.TransformationDto;
import org.springframework.stereotype.Component;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

@Component
@Slf4j
public class Aggregate implements Transformation  {
    public Dataset<Row> apply(TransformationDto transformationDto, List<Dataset<Row>> datasetList){
        Dataset<Row> df = datasetList.get(0);
        Column[] groupByCols = Arrays.asList(transformationDto.getGroupByCol().split(","))
                .stream().map(x -> col(x)).toArray(Column[]::new);
        Map<String,String> operationAndColumnMap = new HashMap<>();
        transformationDto.getApplyOn().stream()
                .forEach(x-> operationAndColumnMap.put(x.getColumn(),x.getOperation()));
        log.info("aggregate called with parameters :: group by - {} and aggregate by - {}",transformationDto.getGroupByCol(),operationAndColumnMap);
        df = df.groupBy(groupByCols).agg(operationAndColumnMap);
        for(OperationAndColumn operationAndColumn: transformationDto.getApplyOn()){
            df = df.withColumnRenamed(String.format("%s(%s)",operationAndColumn.getOperation(),operationAndColumn.getColumn()), operationAndColumn.getOutputColumn());

        }
        return df;

    }
}
