package org.example.spark.transformationBeans;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.example.spark.dataLineageDto.JoinDto;
import org.example.spark.utils.TraceColumn;
import org.example.spark.dto.TransformationInputDto;
import org.example.spark.service.SparkCoreService;
import org.example.spark.service.SparkSessionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Stream;

@Component
@Slf4j
public class Join implements Transformation {
    @Autowired
    SparkSessionService sparkSessionService;
    @Autowired
    SparkCoreService sparkCoreService;
    @Autowired
    Map<String, List<TraceColumn>> columnTraceMap;
    public Dataset<Row> apply(TransformationInputDto transformationInputDto, List<Dataset<Row>> datasetList){
        Dataset<Row> df = datasetList.get(0);
        log.info("join called on dataset :: {}", transformationInputDto.getRightDf());
        Dataset<Row> rightDF = sparkCoreService.readDataset(sparkSessionService.getSparkSession(), transformationInputDto.getRightDf(),null);
        Column joinExpression = null;

        for (String rightCol : transformationInputDto.getRightForwardColumn()){
            rightDF = rightDF.withColumnRenamed(rightCol, String.format("%s_%s", transformationInputDto.getRightDf(),rightCol));
        }
        for(int i = 0; i < transformationInputDto.getLeftJoinColumn().size(); i++){
            joinExpression = df.col(transformationInputDto.getLeftJoinColumn().get(i)).equalTo(rightDF.col(String.format("%s_%s", transformationInputDto.getRightDf(), transformationInputDto.getRightJoinColumn().get(i))));

        }
        String[] rightColumns = transformationInputDto.getRightForwardColumn().stream().map(x-> String.format("%s_%s", transformationInputDto.getRightDf(),x)).toList().toArray(new String[]{});
        Column[] columnsPostJoinOp = Stream.concat(Arrays.stream(df.columns()), Arrays.stream(rightColumns)).map(functions::col).toList().toArray(new Column[0]);
        df = df.join(rightDF,joinExpression).select(columnsPostJoinOp);
        traceColumn(transformationInputDto);
        return df;

    }
    public void traceColumn(TransformationInputDto transformationInputDto){
        transformationInputDto.getRightForwardColumn().stream()
                .forEach(x->
                        {
                            String forwardColName = String.format("%s_%s", transformationInputDto.getRightDf(),x);
                            JoinDto joinDto = JoinDto.builder().transformationName("join")
                                    .leftJoinColumn(transformationInputDto.getLeftJoinColumn())
                                    .rightJoinColumn(transformationInputDto.getRightJoinColumn())
                                    .rightDf(transformationInputDto.getRightDf())

                                    .build();

                            TraceColumn traceColumn = TraceColumn.builder().sourceColumn(x).transformationDto(joinDto).build();
                            // if map has input column already, get the list of trace object and append to the current trace column object
                            if (columnTraceMap.containsKey(x)){
                                List<TraceColumn> traceColumnList = columnTraceMap.get(x);
                                List<TraceColumn> newList = new ArrayList<>();
                                newList.add(traceColumn);
                                newList.addAll(traceColumnList);
                                columnTraceMap.putIfAbsent(forwardColName,newList);
                            }else{
                                List<TraceColumn> newList = new ArrayList<>();
                                newList.add(traceColumn);
                                columnTraceMap.putIfAbsent(forwardColName,newList);
                            }


                        });

    }
}
