
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
public class JoinDto implements TransformationDto{
    String transformationName= "join";
    List<String> leftJoinColumn;
    List<String> rightJoinColumn;
    String rightDf;
}
