package org.example.spark.transformationBeans;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.spark.dto.*;
import org.example.spark.utils.CommonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.apache.spark.sql.functions.col;

@Component
@Slf4j
public class Aggregate implements Transformation  {
    @Autowired
    Map<String, List<TraceColumn>> columnTraceMap;
    public Dataset<Row> apply(TransformationInputDto transformationInputDto, List<Dataset<Row>> datasetList){

        Dataset<Row> df = datasetList.get(0);
        Column[] groupByCols = Arrays.asList(transformationInputDto.getGroupByCol().split(","))
                .stream().map(x -> col(x)).toArray(Column[]::new);
        Map<String,String> operationAndColumnMap = new HashMap<>();
        transformationInputDto.getApplyOn().stream()
                .forEach(x-> operationAndColumnMap.put(x.getColumn(),x.getOperation()));
        log.info("aggregate called with parameters :: group by - {} and aggregate by - {}", transformationInputDto.getGroupByCol(),operationAndColumnMap);
        df = df.groupBy(groupByCols).agg(operationAndColumnMap);
        for(OperationAndColumn operationAndColumn: transformationInputDto.getApplyOn()){
            traceColumn(transformationInputDto);
            df = df.withColumnRenamed(String.format("%s(%s)",operationAndColumn.getOperation(),operationAndColumn.getColumn()), operationAndColumn.getOutputColumn());

        }
        return df;

    }
    public void traceColumn(TransformationInputDto transformationInputDto){
        for (OperationAndColumn operationAndColumn: transformationInputDto.getApplyOn()){
            AggregateDto aggregateDto = AggregateDto.builder().transformationName("aggregate")
                        .inputColumn(operationAndColumn.getColumn())
                        .operation(operationAndColumn.getOperation())
                        .groupByCol(transformationInputDto.getGroupByCol())
                        .build();

                TraceColumn traceColumn = TraceColumn.builder().sourceColumn(operationAndColumn.getColumn()).transformationDto(aggregateDto).build();
                // if map has input column already, get the list of trace object and append to the current trace column object
                if (columnTraceMap.containsKey(operationAndColumn.getColumn())){
                    List<TraceColumn> traceColumnList = columnTraceMap.get(operationAndColumn.getColumn());
                    List<TraceColumn> newList = new ArrayList<>();
                    newList.add(traceColumn);
                    newList.addAll(traceColumnList);
                    columnTraceMap.putIfAbsent(operationAndColumn.getOutputColumn(),newList);
                }else{
                    List<TraceColumn> newList = new ArrayList<>();
                    newList.add(traceColumn);
                    columnTraceMap.putIfAbsent(operationAndColumn.getOutputColumn(),newList);
                }


        }
    }
}
