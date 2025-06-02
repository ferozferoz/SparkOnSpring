
package org.example.spark.dataLineageDto;
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
public class JoinDto implements TransformationDto{
    @JsonProperty("transformationName")
    String transformationName= "join";
    @JsonProperty("leftJoinColumn")
    List<String> leftJoinColumn;
    @JsonProperty("rightJoinColumn")
    List<String> rightJoinColumn;
    @JsonProperty("rightDf")
    String rightDf;
}
