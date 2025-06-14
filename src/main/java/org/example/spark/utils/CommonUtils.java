package org.example.spark.utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommonUtils {

    public static Map<String, List<TraceColumn>> traceColumn(Dataset<Row> df, Map<String, List<TraceColumn>> columnTraceMap,Map<String, List<TraceColumn>> previousColumnTrace ){

        Map<String, List<TraceColumn>> returnTraceMap = new HashMap<>();
        for (String column: df.columns()) {
            if (!columnTraceMap.containsKey(column)) {
                TraceColumn traceColumn = TraceColumn.builder().sourceColumn(column).transformationDto(null).build();
                List<TraceColumn> traceColumnList = new ArrayList<>();
                traceColumnList.add(traceColumn);
                returnTraceMap.put(column, traceColumnList);
            }else{
                returnTraceMap.put(column, columnTraceMap.get(column));
            }
        }

        if(!ObjectUtils.isEmpty(previousColumnTrace)) {
            for (var entry : returnTraceMap.entrySet()) {
                TraceColumn lastElement = entry.getValue().get(entry.getValue().size() - 1);
                for (var preEntry : previousColumnTrace.entrySet()) {
                    if (lastElement.getSourceColumn().equalsIgnoreCase(preEntry.getKey())) {
                        List<TraceColumn> prevList = preEntry.getValue();
                        List<TraceColumn> currList = entry.getValue();
                        currList.addAll(prevList);
                        returnTraceMap.put(entry.getKey(), currList);
                    }
                }
            }
        }
        return returnTraceMap;
    }
}
