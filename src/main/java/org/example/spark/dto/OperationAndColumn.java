package org.example.spark.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.springframework.stereotype.Service;

@Getter
@Service
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OperationAndColumn {
    @JsonProperty("operation")
    String operation;
    @JsonProperty("column")
    String column;
    @JsonProperty("output_column")
    String outputColumn;
}
