package com.pipeline.fanout_engine.sink;

import com.pipeline.fanout_engine.sink.limit.TokenBucketRateLimiter;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SinkThrottlingTest {

    @Test
    void testTokenBucketRateLimiter() throws InterruptedException {
        TokenBucketRateLimiter rateLimiter = new TokenBucketRateLimiter(10);
        
        long startTime = System.currentTimeMillis();
        int iterations = 15;

        for (int i = 0; i < iterations; i++) {
            rateLimiter.acquire();
        }

        long duration = System.currentTimeMillis() - startTime;
        
        assertTrue(duration >= 350, "Duration was " + duration + "ms, expected >= 350ms");
    }

    @Test
    void testRateLimiterConcurrencyWithVirtualThreads() throws InterruptedException {
        TokenBucketRateLimiter rateLimiter = new TokenBucketRateLimiter(50);

        int threadCount = 20;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCounter = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        rateLimiter.acquire();
                        successCounter.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        latch.countDown();
                    }
                });
            }
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        long duration = System.currentTimeMillis() - startTime;

        assertEquals(threadCount, successCounter.get());
        assertTrue(duration < 1500, "Should complete quickly on virtual threads");
    }
}
