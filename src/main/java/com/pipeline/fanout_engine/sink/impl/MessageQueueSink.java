package com.pipeline.fanout_engine.sink.impl;

import com.pipeline.fanout_engine.config.AppConfig;
import com.pipeline.fanout_engine.sink.DataSink;
import com.pipeline.fanout_engine.sink.limit.TokenBucketRateLimiter;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageQueueSink implements DataSink {

    private final AppConfig appConfig;
    private TokenBucketRateLimiter rateLimiter;
    private boolean enabled;
    private String topic;

    @PostConstruct
    public void init() {
        if (appConfig.getSinks() != null) {
            appConfig.getSinks().stream()
                    .filter(s -> "MESSAGE_QUEUE".equalsIgnoreCase(s.getType()))
                    .findFirst()
                    .ifPresent(config -> {
                        this.enabled = config.isEnabled();
                        this.rateLimiter = new TokenBucketRateLimiter(config.getRateLimit());
                        this.topic = config.getProperties().getOrDefault("topic", "");
                    });
        }
    }

    @Override
    public String getType() {
        return "MESSAGE_QUEUE";
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void send(Object transformedData) throws Exception {
        if (!enabled) return;

        rateLimiter.acquire();

        long latency = ThreadLocalRandom.current().nextLong(5, 16);
        Thread.sleep(latency);

        log.debug("MQ Sink: Successfully published to topic {}: {}", topic, transformedData);
    }
}
