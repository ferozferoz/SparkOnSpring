package org.example.spark.transformationBeans;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.example.spark.dto.*;
import org.example.spark.utils.CommonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Component
@Slf4j
public class FunctionCall implements Transformation  {
    @Autowired
    Map<String, List<TraceColumn>> columnTraceMap;
    public Dataset<Row> apply(TransformationInputDto transformationInputDto, List<Dataset<Row>> datasetList){
        UDF1<Integer,Integer> add1 = (Integer input) -> input+1;
        Dataset<Row> df = datasetList.get(0);
        df.sparkSession().udf().register("add1", add1, DataTypes.IntegerType);
        Dataset<Row> newDf = df.withColumn(transformationInputDto.getOutputColumn(),callUDF("add1",col(transformationInputDto.getInputCol())));
        traceColumn(transformationInputDto);
        return newDf;
    }

    public void traceColumn(TransformationInputDto transformationInputDto){
        FunctionCallDto functionCallDto = FunctionCallDto.builder().transformationName("add1").build();
        TraceColumn traceColumn = TraceColumn.builder().sourceColumn(transformationInputDto.getInputCol()).transformationDto(functionCallDto).build();
        if (columnTraceMap.containsKey(transformationInputDto.getInputCol())){
            List<TraceColumn> traceColumnList = columnTraceMap.get(transformationInputDto.getInputCol());
            List<TraceColumn> newList = new ArrayList<>();
            newList.add(traceColumn);
            newList.addAll(traceColumnList);
            columnTraceMap.putIfAbsent(transformationInputDto.getOutputColumn(),newList);
        }else{
            List<TraceColumn> newList = new ArrayList<>();
            newList.add(traceColumn);
            columnTraceMap.putIfAbsent(transformationInputDto.getOutputColumn(),newList);
        }


        }
    }

