package org.example.spark.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.springframework.stereotype.Service;

import java.util.List;

@Getter
@Service
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TransformationDto {
    @JsonProperty("transformation_name")
    String transformationName;
    /* aggregate function attributes */
    @JsonProperty("group_by_col")
    String groupByCol;
    @JsonProperty("apply_on_cols")
    List<OperationAndColumn> applyOn;
    @JsonProperty("aggregate_function")
    String aggregateFunction;
    /* filter function attributes */
    @JsonProperty("filter_expr")
    String filterExpr;
    @JsonProperty("value")
    String value;
    @JsonProperty("df_list")
    List<String> dfList;
    @JsonProperty("left_join_column")
    List<String> leftJoinColumn;
    @JsonProperty("right_join_column")
    List<String> rightJoinColumn;
    @JsonProperty("right_forward_column")
    List<String> rightForwardColumn;
    @JsonProperty("right_df")
    String rightDf;




}
