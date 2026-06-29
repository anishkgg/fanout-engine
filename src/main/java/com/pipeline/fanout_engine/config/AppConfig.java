package com.pipeline.fanout_engine.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import java.util.List;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "pipeline")
public class AppConfig {
    private SourceConfig source;
    private List<SinkConfig> sinks;

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public com.fasterxml.jackson.dataformat.xml.XmlMapper xmlMapper() {
        return new com.fasterxml.jackson.dataformat.xml.XmlMapper();
    }

    @Data
    public static class SourceConfig {
        private String filePath;
        private String format;
        private List<FieldConfig> fixedWidthFields;
    }

    @Data
    public static class FieldConfig {
        private String name;
        private int length;
    }

    @Data
    public static class SinkConfig {
        private String type;
        private boolean enabled;
        private int rateLimit;
        private Map<String, String> properties;
    }
}
