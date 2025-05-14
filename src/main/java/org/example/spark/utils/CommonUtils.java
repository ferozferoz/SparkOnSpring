package org.example.spark.utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.spark.dto.*;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommonUtils {

    public static void traceColumn(Dataset<Row> df, Map<String, List<TraceColumn>> columnTraceMap,Map<String, List<TraceColumn>> previousColumnTrace ){

        for (String column: df.columns()) {
            if (!columnTraceMap.containsKey(column)) {
                TraceColumn traceColumn = TraceColumn.builder().sourceColumn(column).transformationDto(null).build();
                List<TraceColumn> traceColumnList = new ArrayList<>();
                traceColumnList.add(traceColumn);
                columnTraceMap.put(column, traceColumnList);
            }
        }

        if(!ObjectUtils.isEmpty(previousColumnTrace)) {
            for (var entry : columnTraceMap.entrySet()) {
                TraceColumn lastElement = entry.getValue().get(entry.getValue().size() - 1);
                for (var preEntry : previousColumnTrace.entrySet()) {
                    if (lastElement.getSourceColumn().equalsIgnoreCase(preEntry.getKey())) {
                        List<TraceColumn> prevList = preEntry.getValue();
                        List<TraceColumn> currList = entry.getValue();
                        currList.addAll(prevList);
                        columnTraceMap.put(entry.getKey(), currList);
                    }
                }
            }
        }
    }
}
