package com.pipeline.fanout_engine.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.fanout_engine.config.AppConfig;
import com.pipeline.fanout_engine.ingestion.impl.StreamingFileReader;
import com.pipeline.fanout_engine.model.RecordModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class StreamingFileReaderTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testParseJsonl(@TempDir Path tempDir) throws Exception {
        Path jsonlFile = tempDir.resolve("input.jsonl");
        List<String> lines = List.of(
                "{\"id\":\"1\",\"name\":\"John\",\"age\":30}",
                "{\"id\":\"2\",\"name\":\"Alice\",\"age\":25}"
        );
        Files.write(jsonlFile, lines);

        AppConfig appConfig = new AppConfig();
        AppConfig.SourceConfig sourceConfig = new AppConfig.SourceConfig();
        sourceConfig.setFilePath(jsonlFile.toString());
        sourceConfig.setFormat("JSONL");
        appConfig.setSource(sourceConfig);

        try (StreamingFileReader reader = new StreamingFileReader(appConfig, objectMapper);
             Stream<RecordModel> stream = reader.readRecords()) {
            
            List<RecordModel> records = stream.collect(Collectors.toList());
            assertEquals(2, records.size());
            
            assertEquals("1", records.get(0).getId());
            assertEquals("John", records.get(0).getData().get("name"));
            assertEquals(30, records.get(0).getData().get("age"));

            assertEquals("2", records.get(1).getId());
            assertEquals("Alice", records.get(1).getData().get("name"));
            assertEquals(25, records.get(1).getData().get("age"));
        }
    }

    @Test
    void testParseCsv(@TempDir Path tempDir) throws Exception {
        Path csvFile = tempDir.resolve("input.csv");
        List<String> lines = List.of(
                "id,name,city",
                "101,Bob,Boston",
                "102,\"Eve, Secret Agent\",New York"
        );
        Files.write(csvFile, lines);

        AppConfig appConfig = new AppConfig();
        AppConfig.SourceConfig sourceConfig = new AppConfig.SourceConfig();
        sourceConfig.setFilePath(csvFile.toString());
        sourceConfig.setFormat("CSV");
        appConfig.setSource(sourceConfig);

        try (StreamingFileReader reader = new StreamingFileReader(appConfig, objectMapper);
             Stream<RecordModel> stream = reader.readRecords()) {
            
            List<RecordModel> records = stream.collect(Collectors.toList());
            assertEquals(2, records.size());
            
            assertEquals("101", records.get(0).getId());
            assertEquals("Bob", records.get(0).getData().get("name"));
            assertEquals("Boston", records.get(0).getData().get("city"));

            assertEquals("102", records.get(1).getId());
            assertEquals("Eve, Secret Agent", records.get(1).getData().get("name"));
            assertEquals("New York", records.get(1).getData().get("city"));
        }
    }

    @Test
    void testParseFixedWidth(@TempDir Path tempDir) throws Exception {
        Path fwFile = tempDir.resolve("input.txt");
        List<String> lines = List.of(
                "1001      Charlie   ",
                "1002      Diana     "
        );
        Files.write(fwFile, lines);

        AppConfig appConfig = new AppConfig();
        AppConfig.SourceConfig sourceConfig = new AppConfig.SourceConfig();
        sourceConfig.setFilePath(fwFile.toString());
        sourceConfig.setFormat("FIXED_WIDTH");
        
        AppConfig.FieldConfig f1 = new AppConfig.FieldConfig();
        f1.setName("id");
        f1.setLength(10);
        
        AppConfig.FieldConfig f2 = new AppConfig.FieldConfig();
        f2.setName("name");
        f2.setLength(10);
        
        sourceConfig.setFixedWidthFields(List.of(f1, f2));
        appConfig.setSource(sourceConfig);

        try (StreamingFileReader reader = new StreamingFileReader(appConfig, objectMapper);
             Stream<RecordModel> stream = reader.readRecords()) {
            
            List<RecordModel> records = stream.collect(Collectors.toList());
            assertEquals(2, records.size());
            
            assertEquals("1001", records.get(0).getId());
            assertEquals("Charlie", records.get(0).getData().get("name"));

            assertEquals("1002", records.get(1).getId());
            assertEquals("Diana", records.get(1).getData().get("name"));
        }
    }
}
