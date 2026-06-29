package com.pipeline.fanout_engine.observability;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

@Component
public class MetricsCollector {
    private final LongAdder totalProcessed = new LongAdder();
    private final Map<String, LongAdder> sinkSuccessCounters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> sinkFailureCounters = new ConcurrentHashMap<>();

    public void incrementProcessed() {
        totalProcessed.increment();
    }

    public long getTotalProcessed() {
        return totalProcessed.sum();
    }

    public void incrementSuccess(String sinkType) {
        sinkSuccessCounters.computeIfAbsent(sinkType, k -> new LongAdder()).increment();
    }

    public long getSuccessCount(String sinkType) {
        LongAdder counter = sinkSuccessCounters.get(sinkType);
        return counter != null ? counter.sum() : 0;
    }

    public void incrementFailure(String sinkType) {
        sinkFailureCounters.computeIfAbsent(sinkType, k -> new LongAdder()).increment();
    }

    public long getFailureCount(String sinkType) {
        LongAdder counter = sinkFailureCounters.get(sinkType);
        return counter != null ? counter.sum() : 0;
    }
}
