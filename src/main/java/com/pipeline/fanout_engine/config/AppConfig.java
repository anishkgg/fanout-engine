package com.pipeline.fanout_engine.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "pipeline")
public class AppConfig {
    private SourceConfig source;
    private List<SinkConfig> sinks;

    @Data
    public static class SourceConfig {
        private String filePath;
        private String format;
    }

    @Data
    public static class SinkConfig {
        private String type;
        private boolean enabled;
        private int rateLimit;
        private Map<String, String> properties;
    }
}
