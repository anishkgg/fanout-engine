package com.pipeline.fanout_engine.ingestion;

import com.pipeline.fanout_engine.model.RecordModel;
import java.io.IOException;
import java.util.stream.Stream;

public interface RecordReader extends AutoCloseable {
    Stream<RecordModel> readRecords() throws IOException;
}
