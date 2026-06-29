package com.pipeline.fanout_engine.ingestion.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.fanout_engine.config.AppConfig;
import com.pipeline.fanout_engine.ingestion.RecordReader;
import com.pipeline.fanout_engine.model.RecordModel;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Slf4j
public class StreamingFileReader implements RecordReader {

    private final String filePath;
    private final String format;
    private final List<AppConfig.FieldConfig> fixedWidthFields;
    private final ObjectMapper objectMapper;
    private final BufferedReader reader;
    private final AtomicLong recordCounter = new AtomicLong(0);
    private String[] csvHeaders;

    public StreamingFileReader(AppConfig appConfig, ObjectMapper objectMapper) throws IOException {
        this.filePath = appConfig.getSource().getFilePath();
        this.format = appConfig.getSource().getFormat().toUpperCase();
        this.fixedWidthFields = appConfig.getSource().getFixedWidthFields();
        this.objectMapper = objectMapper;
        this.reader = Files.newBufferedReader(Paths.get(filePath));
        log.info("Opening file for streaming: {} with format: {}", this.filePath, this.format);
        
        if ("CSV".equals(format)) {
            // Read first line as CSV headers
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IOException("CSV file is empty!");
            }
            this.csvHeaders = parseCsvLine(headerLine);
        }
    }

    @Override
    public Stream<RecordModel> readRecords() {
        return reader.lines().map(line -> {
            try {
                Map<String, Object> data = parseLine(line);
                if (data == null) {
                    return null;
                }
                long recordNum = recordCounter.incrementAndGet();
                String id = (data.containsKey("id")) ? String.valueOf(data.get("id")) : UUID.randomUUID().toString();
                return RecordModel.builder()
                        .id(id)
                        .recordNumber(recordNum)
                        .data(data)
                        .retryCount(0)
                        .build();
            } catch (Exception e) {
                log.error("Failed to parse line: {}", line, e);
                return null;
            }
        }).filter(Objects::nonNull);
    }

    private Map<String, Object> parseLine(String line) throws Exception {
        if (line == null || line.trim().isEmpty()) {
            return null;
        }
        switch (format) {
            case "JSONL":
                return objectMapper.readValue(line, new TypeReference<Map<String, Object>>() {});
            case "CSV":
                return parseCsv(line);
            case "FIXED_WIDTH":
                return parseFixedWidth(line);
            default:
                throw new IllegalArgumentException("Unsupported source format: " + format);
        }
    }

    private Map<String, Object> parseCsv(String line) {
        String[] values = parseCsvLine(line);
        Map<String, Object> data = new LinkedHashMap<>();
        for (int i = 0; i < csvHeaders.length; i++) {
            String value = (i < values.length) ? values[i].trim() : "";
            if (value.startsWith("\"") && value.endsWith("\"") && value.length() > 1) {
                value = value.substring(1, value.length() - 1);
            }
            data.put(csvHeaders[i].trim(), value);
        }
        return data;
    }

    private String[] parseCsvLine(String line) {
        return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    }

    private Map<String, Object> parseFixedWidth(String line) {
        Map<String, Object> data = new LinkedHashMap<>();
        if (fixedWidthFields == null || fixedWidthFields.isEmpty()) {
            log.warn("Fixed-width fields are not configured! Storing line as raw content.");
            data.put("raw", line);
            return data;
        }
        int start = 0;
        for (AppConfig.FieldConfig field : fixedWidthFields) {
            if (start >= line.length()) {
                data.put(field.getName(), "");
                continue;
            }
            int end = Math.min(line.length(), start + field.getLength());
            String value = line.substring(start, end).trim();
            data.put(field.getName(), value);
            start = end;
        }
        return data;
    }

    @Override
    public void close() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }
}
