package org.example.spark.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import org.example.spark.utils.TraceColumn;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class WriteRequest {
    String dataset;
    String date;
    Map<String, Map<String,List<TransformationInputDto>>> transformationDag;
    Map<String, List<TraceColumn>> columnTrace;

}
