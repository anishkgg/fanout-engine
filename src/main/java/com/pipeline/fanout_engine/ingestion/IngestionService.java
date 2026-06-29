package com.pipeline.fanout_engine.ingestion;

import java.io.IOException;

public interface IngestionService {
    RecordReader createReader() throws IOException;
}
