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
public class Union implements Transformation {
    public Dataset<Row> apply(TransformationDto transformationDto, List<Dataset<Row>> datasetList){

        Dataset<Row> dataset;
        if (datasetList.size() < 2){
            log.error("union cannot be performed returning first dataframe");
            dataset = datasetList.get(0);
        } else if (datasetList.size() == 2) {
            dataset = datasetList.get(0).union(datasetList.get(1));
        }else{
            dataset = datasetList.get(0).union(datasetList.get(1));
            for (int i=2;i<datasetList.size()-1;i++){
                dataset = dataset.union(datasetList.get(i));
            }
        }
        return dataset;
    }
}
