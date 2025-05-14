package org.example.spark.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.spark.dto.TraceColumn;
import org.json.JSONObject;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.example.spark.dto.TransformationDto;
import org.example.spark.dto.TransformationInputDto;
import org.example.spark.dto.WriteRequest;
import org.example.spark.transformationBeans.Transformation;
import org.example.spark.utils.CommonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class SparkCoreService {
    @Autowired
    SparkSessionService sparkSessionService;
    @Autowired
    private ApplicationContext applicationContext;
    @Autowired
    Map<String, List<TraceColumn>> columnTraceMap;

    public Dataset<Row> transform (WriteRequest writeRequest, Dataset<Row> dataset) throws JsonProcessingException {
        Map<String, Dataset<Row>> datasetMap = new HashMap<>();
        datasetMap.put("ROOT",dataset);
        Map<String, Map<String,List<TransformationInputDto>>> transformationDag = writeRequest.getTransformationDag();
        for (Map.Entry<String, Map<String,List<TransformationInputDto>>> dagEntry : transformationDag.entrySet()) {
            for (Map.Entry<String, List<TransformationInputDto>> entry : dagEntry.getValue().entrySet()) {
                List<Dataset<Row>> datasetList = new ArrayList<>();
                for(String datasetName:entry.getKey().split(",")){
                    dataset = datasetMap.get(datasetName);
                    datasetList.add(dataset);
                }

                for (TransformationInputDto transformationInputDto : entry.getValue()){
                    Transformation transformation = applicationContext.getBean(transformationInputDto.getTransformationName(), Transformation.class);
                    log.info("Operation called :: {} on dataset :: {}", transformationInputDto.getTransformationName(),entry.getKey());
                    dataset = transformation.apply(transformationInputDto,datasetList);
                    dataset.show(false);
                }
                log.info("new dataset generated :: {} on input dataset : {} ", dagEntry.getKey(),entry.getKey());
                datasetMap.put(dagEntry.getKey(),dataset);
            }

        }
        System.out.println(columnTraceMap);
        CommonUtils.traceColumn(dataset,columnTraceMap,writeRequest.getColumnTrace());

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonString = objectMapper.writeValueAsString(columnTraceMap);
        System.out.println("columnTraceMap>>>>>" +jsonString);
        //System.out.println("columnTraceMap>>>>>" + new JSONObject(columnTraceMap));
        return dataset;
    }
    public void writeData(WriteRequest writeRequest) throws Exception{
        try {
            columnTraceMap.clear();
            Dataset<Row> dataset = readDataset(sparkSessionService.getSparkSession(), writeRequest.getDataset(), writeRequest.getDate());
            dataset.show(false);
            transform(writeRequest,dataset);

            //dataset.write().mode(SaveMode.Overwrite).csv("./output");
        }catch (Exception ex){
            ex.printStackTrace();
            throw new Exception("write failed");
        }
    }

    public Dataset<Row> readDataset(SparkSession sparkSession, String dataset, String cobDate){
        StructType structType = new StructType();
        structType = structType.add("serialNo", DataTypes.IntegerType, false);
        structType = structType.add("name", DataTypes.StringType, false);
        structType = structType.add("city", DataTypes.StringType, false);
        structType = structType.add("age", DataTypes.IntegerType, false);
        structType = structType.add("weight", DataTypes.DoubleType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create(1,"feroz","hyderabad",41,70.5));
        nums.add(RowFactory.create(2,"akram","jamshedpur",50,80.0));
        nums.add(RowFactory.create(3,"gaurav","Ipswich",41,75.3));
        nums.add(RowFactory.create(4,"Umar","birmingham city",22,60.4));
        nums.add(RowFactory.create(5,"Mansoor","hyderabad",41,80.0));
        nums.add(RowFactory.create(6,"Ambreen","hyderabad",36,55.5));
        Dataset<Row> df = sparkSession.createDataFrame(nums, structType);

        StructType structType1 = new StructType();
        structType1 = structType1.add("city", DataTypes.StringType, false);
        structType1 = structType1.add("state", DataTypes.StringType, false);
        structType1 = structType1.add("country", DataTypes.StringType, false);

        List<Row> nums1 = new ArrayList<Row>();
        nums1.add(RowFactory.create("hyderabad","Telangana","India"));
        nums1.add(RowFactory.create("jamshedpur","Jharkhand","India"));
        nums1.add(RowFactory.create("Ipswich","birmingham","United Kingdom"));
        nums1.add(RowFactory.create("birmingham city","birmingham","United Kingdom"));
        Dataset<Row> df1 = sparkSession.createDataFrame(nums1, structType1);

        if ("dummy_dataset".equalsIgnoreCase(dataset)){
            return df;
        } else if ("dummy_dataset_1".equalsIgnoreCase(dataset)) {
            return df1;
        }else {
            return df;
        }

    }
}
