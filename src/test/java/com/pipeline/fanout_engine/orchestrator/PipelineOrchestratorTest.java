package com.pipeline.fanout_engine.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.fanout_engine.config.AppConfig;
import com.pipeline.fanout_engine.observability.MetricsCollector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class PipelineOrchestratorTest {

    @Autowired
    private PipelineOrchestrator orchestrator;

    @Autowired
    private AppConfig appConfig;

    @Autowired
    private MetricsCollector metricsCollector;

    @Test
    void testEndToEndPipeline(@TempDir Path tempDir) throws Exception {
        Path jsonlFile = tempDir.resolve("input.jsonl");
        List<String> lines = List.of(
                "{\"id\":\"rec-1\",\"name\":\"Alice\",\"age\":30}",
                "{\"id\":\"rec-2\",\"name\":\"Bob\",\"age\":25}",
                "{\"id\":\"rec-3\",\"name\":\"Charlie\",\"age\":35}"
        );
        Files.write(jsonlFile, lines);

        appConfig.getSource().setFilePath(jsonlFile.toString());
        appConfig.getSource().setFormat("JSONL");

        orchestrator.runPipeline();

        long totalProcessed = metricsCollector.getTotalProcessed();
        assertTrue(totalProcessed >= 3, "Expected to process 3 records, got: " + totalProcessed);

        assertEquals(totalProcessed, metricsCollector.getSuccessCount("REST"));
        assertEquals(totalProcessed, metricsCollector.getSuccessCount("GRPC"));
        assertEquals(totalProcessed, metricsCollector.getSuccessCount("MESSAGE_QUEUE"));
        assertEquals(totalProcessed, metricsCollector.getSuccessCount("DB"));
    }
}
