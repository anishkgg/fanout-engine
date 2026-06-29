package com.pipeline.fanout_engine.ingestion.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.fanout_engine.config.AppConfig;
import com.pipeline.fanout_engine.ingestion.IngestionService;
import com.pipeline.fanout_engine.ingestion.RecordReader;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@RequiredArgsConstructor
public class IngestionServiceImpl implements IngestionService {

    private final AppConfig appConfig;
    private final ObjectMapper objectMapper;

    @Override
    public RecordReader createReader() throws IOException {
        return new StreamingFileReader(appConfig, objectMapper);
    }
}
