package org.example.spark.dataLineageDto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;

@JsonDeserialize(as = TransformationDtoImpl.class)
public interface TransformationDto {
}
