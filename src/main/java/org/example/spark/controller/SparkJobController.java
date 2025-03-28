package org.example.spark.controller;

import org.example.spark.dto.WriteRequest;
import org.example.spark.service.SparkCoreService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/api")
public class SparkJobController {
    @Autowired
    SparkCoreService sparkCoreService;

    @PostMapping("/write_data")
    public ResponseEntity<String> write(@RequestBody WriteRequest writeRequest) {
        try{
            sparkCoreService.writeData(writeRequest);
            return new ResponseEntity<>(HttpStatus.CREATED);
        }catch (Exception ex){
            return new ResponseEntity<>("Write Failed.. " + ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

}