package org.example.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;

@JsonDeserialize(as = TransformationDtoImpl.class)
public interface TransformationDto {
}
@Getter
class TransformationDtoImpl implements TransformationDto {
    @JsonProperty("transformationName")
    String transformationName = "aggregate";
}