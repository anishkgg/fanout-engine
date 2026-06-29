package com.pipeline.fanout_engine.sink.limit;

public class TokenBucketRateLimiter {
    private final long capacity;
    private final double refillRatePerMs;
    
    private double tokens;
    private long lastRefillTimestamp;

    public TokenBucketRateLimiter(int ratePerSecond) {
        this.capacity = ratePerSecond;
        this.tokens = ratePerSecond;
        this.refillRatePerMs = (double) ratePerSecond / 1000.0;
        this.lastRefillTimestamp = System.currentTimeMillis();
    }

    public synchronized void acquire() throws InterruptedException {
        if (capacity <= 0) {
            return; // No rate limiting if set to 0 or negative
        }
        while (true) {
            refill();
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return;
            }
            double missingTokens = 1.0 - tokens;
            long waitTimeMs = (long) Math.ceil(missingTokens / refillRatePerMs);
            if (waitTimeMs > 0) {
                Thread.sleep(waitTimeMs);
            } else {
                Thread.sleep(1);
            }
        }
    }

    private void refill() {
        long now = System.currentTimeMillis();
        long elapsedMs = now - lastRefillTimestamp;
        if (elapsedMs > 0) {
            double refillAmount = elapsedMs * refillRatePerMs;
            tokens = Math.min(capacity, tokens + refillAmount);
            lastRefillTimestamp = now;
        }
    }

    public synchronized double getTokens() {
        refill();
        return tokens;
    }
}
