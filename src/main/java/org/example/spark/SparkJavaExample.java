package org.example.spark;
// Create Java RDD Example
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkJavaExample {
    public static void main(String args[]){

        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("PersonDatabase")
                .master("local[*]")
                .getOrCreate();
        StructType structType = new StructType();
        structType = structType.add("serialNo", DataTypes.IntegerType, false);
        structType = structType.add("name", DataTypes.StringType, false);
        structType = structType.add("city", DataTypes.StringType, false);
        structType = structType.add("age", DataTypes.IntegerType, false);
        structType = structType.add("weight", DataTypes.DoubleType, false);
        structType = structType.add("state", DataTypes.StringType, false);
        structType = structType.add("country", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create(1,"feroz","hyderabad",41,70.5,"Telangana","India"));
        nums.add(RowFactory.create(1,"akram","jamshedpur",50,80.0,"Jharkhand","India"));
        nums.add(RowFactory.create(1,"gaurav","Ipswich",41,75.3,"birmingham","United Kingdom"));
        nums.add(RowFactory.create(1,"Umar","birmingham city",22,60.4,"birmingham","United Kingdom"));
        nums.add(RowFactory.create(1,"Mansoor","hyderabad",41,80.0,"Telangana","India"));
        nums.add(RowFactory.create(1,"Ambreen","hyderabad",36,55.5,"Telangana","India"));
        Dataset<Row> df = spark.createDataFrame(nums, structType);
        df.show(false);
    }
}