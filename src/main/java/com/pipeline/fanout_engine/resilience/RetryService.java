package com.pipeline.fanout_engine.resilience;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.Callable;

@Slf4j
@Service
public class RetryService {

    public <T> T executeWithRetry(Callable<T> task, String description, int maxRetries, long initialBackoffMs) throws Exception {
        int attempt = 0;
        long backoff = initialBackoffMs;

        while (true) {
            try {
                return task.call();
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries) {
                    log.error("Task '{}' failed after {} attempts. Error: {}", description, attempt, e.getMessage());
                    throw e;
                }
                log.warn("Attempt {} of {} failed for '{}'. Retrying in {}ms... Error: {}", 
                         attempt, maxRetries, description, backoff, e.getMessage());
                Thread.sleep(backoff);
                backoff *= 2;
            }
        }
    }
}
