package com.pipeline.fanout_engine.resilience;

import com.pipeline.fanout_engine.config.AppConfig;
import com.pipeline.fanout_engine.orchestrator.PipelineOrchestrator;
import com.pipeline.fanout_engine.sink.DataSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest
class ResilienceIntegrationTest {

    @Autowired
    private PipelineOrchestrator orchestrator;

    @Autowired
    private AppConfig appConfig;

    @MockitoSpyBean
    private com.pipeline.fanout_engine.sink.impl.WideColumnDbSink dbSink;

    @BeforeEach
    void cleanDlq() {
        File dlqFile = new File("data/dlq.jsonl");
        if (dlqFile.exists()) {
            dlqFile.delete();
        }
    }

    @Test
    void testFailureRoutesToDlq(@TempDir Path tempDir) throws Exception {
        Path jsonlFile = tempDir.resolve("input.jsonl");
        Files.write(jsonlFile, List.of("{\"id\":\"bad-rec\",\"name\":\"Broken Record\"}"));

        appConfig.getSource().setFilePath(jsonlFile.toString());
        appConfig.getSource().setFormat("JSONL");

        doThrow(new RuntimeException("Simulated Database Crash")).when(dbSink).send(any());

        orchestrator.runPipeline();

        // 1 initial attempt + 3 retries = 4 invocations
        verify(dbSink, times(4)).send(any());

        File dlqFile = new File("data/dlq.jsonl");
        assertTrue(dlqFile.exists(), "DLQ file should be created");

        List<String> dlqLines = Files.readAllLines(dlqFile.toPath());
        assertFalse(dlqLines.isEmpty(), "DLQ should contain failed record");

        String firstDlqLine = dlqLines.get(0);
        assertTrue(firstDlqLine.contains("Simulated Database Crash"));
        assertTrue(firstDlqLine.contains("bad-rec"));
    }
}
