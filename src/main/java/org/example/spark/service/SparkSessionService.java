package org.example.spark.service;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
@Component
public class SparkSessionService {
    SparkSession sparkSession;
    public SparkSessionService(){
        sparkSession= SparkSession.builder()
                .appName("PersonDatabase")
                .master("local[*]")
                .getOrCreate();
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }
}
