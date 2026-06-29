package com.pipeline.fanout_engine.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RecordModel {
    private String id;
    private long recordNumber;
    private Map<String, Object> data;
    private int retryCount;
}
