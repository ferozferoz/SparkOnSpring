package org.example.spark.config;
import org.example.spark.dto.TraceColumn;
import org.example.spark.dto.TransformationDto;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class AppConfig {

    @Bean
    public Map<String, List<TraceColumn>> columnTraceMap() {
        Map<String, List<TraceColumn>> map = new HashMap<>();
        return map;
    }
}