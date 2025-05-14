
package org.example.spark.dto;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.springframework.stereotype.Service;
import java.util.List;

@Getter
@Setter
@Service
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class AggregateDto implements TransformationDto{
    @JsonProperty("transformationName")
    String transformationName = "aggregate";
    @JsonProperty("groupByCol")
    String groupByCol;
    @JsonProperty("operation")
    String operation;
    @JsonProperty("inputColumn")
    String inputColumn;


}
