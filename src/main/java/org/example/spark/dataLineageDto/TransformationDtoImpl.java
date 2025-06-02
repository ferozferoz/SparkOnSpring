package org.example.spark.dataLineageDto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.springframework.stereotype.Service;

@Getter
@Setter
@Service
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString

class TransformationDtoImpl implements TransformationDto {
    @JsonProperty("transformationName")
    String transformationName = "aggregate";
}
