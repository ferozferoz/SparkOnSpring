package org.example.spark.transformationBeans;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Right;
import org.apache.spark.sql.functions;
import org.example.spark.dto.TransformationDto;
import org.example.spark.service.SparkCoreService;
import org.example.spark.service.SparkSessionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;

@Component
@Slf4j
public class Join implements Transformation {
    @Autowired
    SparkSessionService sparkSessionService;
    @Autowired
    SparkCoreService sparkCoreService;
    public Dataset<Row> apply(TransformationDto transformationDto, List<Dataset<Row>> datasetList){
        Dataset<Row> df = datasetList.get(0);
        log.info("join called on dataset :: {}",transformationDto.getRightDf());
        Dataset<Row> rightDF = sparkCoreService.readDataset(sparkSessionService.getSparkSession(),transformationDto.getRightDf(),null);
        Column joinExpression = null;

        for (String rightCol : transformationDto.getRightForwardColumn()){
            rightDF = rightDF.withColumnRenamed(rightCol, String.format("%s_%s",transformationDto.getRightDf(),rightCol));
        }
        df.show(false);
        rightDF.show(false);
        for(int i=0; i < transformationDto.getLeftJoinColumn().size(); i++){
            joinExpression = df.col(transformationDto.getLeftJoinColumn().get(i)).equalTo(rightDF.col(String.format("%s_%s",transformationDto.getRightDf(),transformationDto.getRightJoinColumn().get(i))));

        }
        String[] rightColumns = transformationDto.getRightForwardColumn().stream().map(x-> String.format("%s_%s",transformationDto.getRightDf(),x)).toList().toArray(new String[]{});
        System.out.println(">>>>>>rightColumns"+Arrays.stream(rightColumns).toList());
        Column[] columnsPostJoinOp = Stream.concat(Arrays.stream(df.columns()), Arrays.stream(rightColumns)).map(functions::col).toList().toArray(new Column[0]);
        System.out.println(">>>>>>columnsPostJoinOp"+Arrays.stream(columnsPostJoinOp).toList());

        df = df.join(rightDF,joinExpression).select(columnsPostJoinOp);
        return df;

    }
}
