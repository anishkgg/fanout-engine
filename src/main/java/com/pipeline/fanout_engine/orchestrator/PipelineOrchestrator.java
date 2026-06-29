package com.pipeline.fanout_engine.orchestrator;

import com.pipeline.fanout_engine.ingestion.IngestionService;
import com.pipeline.fanout_engine.ingestion.RecordReader;
import com.pipeline.fanout_engine.model.RecordModel;
import com.pipeline.fanout_engine.observability.MetricsCollector;
import com.pipeline.fanout_engine.sink.DataSink;
import com.pipeline.fanout_engine.transformer.RecordTransformer;
import com.pipeline.fanout_engine.transformer.TransformerFactory;
import com.pipeline.fanout_engine.resilience.DeadLetterQueue;
import com.pipeline.fanout_engine.resilience.RetryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class PipelineOrchestrator {

    private final IngestionService ingestionService;
    private final List<DataSink> sinks;
    private final TransformerFactory transformerFactory;
    private final MetricsCollector metricsCollector;
    private final RetryService retryService;
    private final DeadLetterQueue deadLetterQueue;

    private final Semaphore inFlightSemaphore = new Semaphore(1000);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public void runPipeline() throws Exception {
        List<DataSink> enabledSinks = sinks.stream().filter(DataSink::isEnabled).toList();
        if (enabledSinks.isEmpty()) {
            log.warn("No sinks are enabled in configuration! Exiting pipeline.");
            return;
        }

        log.info("Starting pipeline with enabled sinks: {}", 
                 enabledSinks.stream().map(DataSink::getType).toList());

        long startTime = System.currentTimeMillis();
        startMetricReporting(startTime, enabledSinks);

        try (ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
             RecordReader reader = ingestionService.createReader();
             Stream<RecordModel> recordStream = reader.readRecords()) {

            recordStream.forEach(record -> {
                try {
                    inFlightSemaphore.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Pipeline interrupted", e);
                }

                 virtualExecutor.submit(() -> {
                    try {
                        dispatchRecordToSinks(record, enabledSinks);
                        metricsCollector.incrementProcessed();
                    } finally {
                        inFlightSemaphore.release();
                    }
                });
            });

            log.info("All records read. Awaiting completion of all downstream tasks...");
            virtualExecutor.shutdown();
            virtualExecutor.awaitTermination(1, TimeUnit.HOURS);
        } finally {
            scheduler.shutdown();
            deadLetterQueue.close();
            log.info("Pipeline run finished.");
        }
    }

    private void dispatchRecordToSinks(RecordModel record, List<DataSink> enabledSinks) {
        List<CompletableFuture<Void>> futures = enabledSinks.stream().map(sink -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> {
                String sinkType = sink.getType();
                try {
                    RecordTransformer transformer = transformerFactory.getTransformer(sinkType)
                            .orElseThrow(() -> new IllegalArgumentException("No transformer found for sink " + sinkType));
                    
                    Object transformed = transformer.transform(record);

                    retryService.executeWithRetry(() -> {
                        sink.send(transformed);
                        return null;
                    }, "Send to " + sinkType, 3, 100);

                    metricsCollector.incrementSuccess(sinkType);
                    future.complete(null);
                } catch (Throwable t) {
                    metricsCollector.incrementFailure(sinkType);
                    log.error("Failed to send record {} to sink {} after retries: {}", 
                              record.getId(), sinkType, t.getMessage());
                    deadLetterQueue.writeToDlq(record, sinkType, t.getMessage());
                    future.completeExceptionally(t);
                }
            });
            return future;
        }).toList();

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.warn("Some sinks failed to process record {}", record.getId());
        }
    }

    private void startMetricReporting(long startTime, List<DataSink> enabledSinks) {
        final long[] lastProcessed = {0};
        final long[] lastTimestamp = {startTime};

        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            long totalProcessed = metricsCollector.getTotalProcessed();
            long deltaProcessed = totalProcessed - lastProcessed[0];
            long elapsedTimeMs = now - lastTimestamp[0];

            double throughput = (elapsedTimeMs > 0) ? (deltaProcessed * 1000.0) / elapsedTimeMs : 0.0;

            log.info("--- Observability Metrics (5s Update) ---");
            log.info("Total Records Ingested: {}", totalProcessed);
            log.info("Current Throughput: {} records/sec", String.format("%.2f", throughput));
            for (DataSink sink : enabledSinks) {
                String type = sink.getType();
                log.info("Sink: {} -> Success: {}, Failure: {}", 
                         type, 
                         metricsCollector.getSuccessCount(type), 
                         metricsCollector.getFailureCount(type));
            }
            log.info("-----------------------------------------");

            lastProcessed[0] = totalProcessed;
            lastTimestamp[0] = now;
        }, 5, 5, TimeUnit.SECONDS);
    }
}
