package com.pipeline.fanout_engine.sink;

public interface DataSink {
    String getType();
    boolean isEnabled();
    void send(Object transformedData) throws Exception;
}
