package org.example.spark.transformationBeans;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.example.spark.dataLineageDto.FunctionCallDto;
import org.example.spark.dto.*;
import org.example.spark.utils.TraceColumn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLOutput;
import java.util.*;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Component
@Slf4j
/*Some specialized complex statistical, mathematical, iterative  function which cannot be implemented using sql operation*/
/*here addition with 1 is demonstrated */

public class FunctionCall implements Transformation {
    @Autowired
    Map<String, List<TraceColumn>> columnTraceMap;

    public Dataset<Row> apply(TransformationInputDto transformationInputDto, List<Dataset<Row>> datasetList) {
        UDF1<Integer, Integer> add1 = (Integer input) -> input + 1;
        Dataset<Row> df = datasetList.get(0);
        df.sparkSession().udf().register("FunctionCall", add1, DataTypes.IntegerType);
        Dataset<Row> newDf = df.withColumn(transformationInputDto.getOutputColumn(), callUDF("FunctionCall", col(transformationInputDto.getInputCol())));
        traceColumn(transformationInputDto);
        return newDf;
    }

    public void traceColumn(TransformationInputDto transformationInputDto) {
        FunctionCallDto functionCallDto = FunctionCallDto.builder().transformationName("FunctionCall").inputColumn(transformationInputDto.getInputCol()).build();
        String sourceColumn = transformationInputDto.getSourceDataFrame() + ":" + transformationInputDto.getInputCol();
        TraceColumn traceColumn = TraceColumn.builder().sourceColumn(sourceColumn).transformationDto(functionCallDto).build();
        System.out.println("><><><"+ columnTraceMap);
        if (columnTraceMap.containsKey(transformationInputDto.getInputCol())){
            List<TraceColumn> traceColumnList = columnTraceMap.get(transformationInputDto.getInputCol());
            List<TraceColumn> newList = new ArrayList<>();
            newList.add(traceColumn);
            newList.addAll(traceColumnList);
            columnTraceMap.put(transformationInputDto.getOutputColumn(), newList);
        } else {
            List<TraceColumn> newList = new ArrayList<>();
            newList.add(traceColumn);
            columnTraceMap.put(transformationInputDto.getOutputColumn(), newList);
        }
    }
}

