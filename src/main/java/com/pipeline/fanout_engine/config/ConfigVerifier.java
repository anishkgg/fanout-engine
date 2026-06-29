package com.pipeline.fanout_engine.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConfigVerifier implements CommandLineRunner {

    private final AppConfig appConfig;

    @Override
    public void run(String... args) throws Exception {
        log.info("------------------------------------------------");
        log.info("Verifying pipeline configuration load:");
        if (appConfig.getSource() != null) {
            log.info("Source Config: FilePath={}, Format={}", 
                     appConfig.getSource().getFilePath(), 
                     appConfig.getSource().getFormat());
        } else {
            log.error("Source configuration is missing!");
        }

        if (appConfig.getSinks() != null && !appConfig.getSinks().isEmpty()) {
            log.info("Loaded Sinks:");
            appConfig.getSinks().forEach(sink -> 
                log.info(" - Type: {}, Enabled: {}, RateLimit: {}, Properties: {}", 
                         sink.getType(), sink.isEnabled(), sink.getRateLimit(), sink.getProperties())
            );
        } else {
            log.warn("No sinks loaded in configuration!");
        }
        log.info("------------------------------------------------");
    }
}
