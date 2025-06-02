package org.example.spark.utils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.example.spark.dataLineageDto.TransformationDto;
import org.springframework.stereotype.Service;

@Getter
@Setter
@Service
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TraceColumn {
    @JsonProperty("sourceColumn")
    String sourceColumn;
    @JsonProperty("transformationDto")
    TransformationDto transformationDto;

}
