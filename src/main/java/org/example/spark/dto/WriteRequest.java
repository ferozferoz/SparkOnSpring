package org.example.spark.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Getter
@Service
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class WriteRequest {
    String dataset;
    String date;
    Map<String, Map<String,List<TransformationDto>>> transformationDag;

}
