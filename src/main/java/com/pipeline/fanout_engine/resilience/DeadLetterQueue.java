package com.pipeline.fanout_engine.resilience;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.fanout_engine.model.RecordModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterQueue {

    private final ObjectMapper objectMapper;
    private BufferedWriter writer;
    private final String dlqFilePath = "data/dlq.jsonl";

    public synchronized void writeToDlq(RecordModel record, String sinkType, String errorMessage) {
        try {
            if (writer == null) {
                Files.createDirectories(Paths.get(dlqFilePath).getParent());
                writer = new BufferedWriter(new FileWriter(dlqFilePath, true));
            }

            Map<String, Object> dlqRecord = new LinkedHashMap<>();
            dlqRecord.put("timestamp", System.currentTimeMillis());
            dlqRecord.put("sinkType", sinkType);
            dlqRecord.put("error", errorMessage);
            dlqRecord.put("record", record);

            String jsonLine = objectMapper.writeValueAsString(dlqRecord);
            writer.write(jsonLine);
            writer.newLine();
            writer.flush();
            log.info("Record {} routed to DLQ at {}", record.getId(), dlqFilePath);
        } catch (IOException e) {
            log.error("Failed to write record {} to DLQ file: {}", record.getId(), e.getMessage());
        }
    }

    public synchronized void close() {
        if (writer != null) {
            try {
                writer.close();
                writer = null;
            } catch (IOException e) {
                log.error("Error closing DLQ writer: {}", e.getMessage());
            }
        }
    }
}
