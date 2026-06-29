package com.pipeline.fanout_engine.transformer.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.fanout_engine.model.RecordModel;
import com.pipeline.fanout_engine.transformer.RecordTransformer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JsonTransformer implements RecordTransformer {

    private final ObjectMapper objectMapper;

    @Override
    public String getSinkType() {
        return "REST";
    }

    @Override
    public String transform(RecordModel record) throws Exception {
        return objectMapper.writeValueAsString(record.getData());
    }
}
