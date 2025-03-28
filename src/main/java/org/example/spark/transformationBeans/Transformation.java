package org.example.spark.transformationBeans;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.spark.dto.TransformationDto;
import org.example.spark.service.SparkSessionService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

public interface Transformation {

    Dataset<Row> apply(TransformationDto transformationDto, List<Dataset<Row>> datasetList );
}
