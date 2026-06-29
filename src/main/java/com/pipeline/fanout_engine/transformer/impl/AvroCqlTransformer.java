package com.pipeline.fanout_engine.transformer.impl;

import com.pipeline.fanout_engine.model.RecordModel;
import com.pipeline.fanout_engine.transformer.RecordTransformer;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class AvroCqlTransformer implements RecordTransformer {

    @Override
    public String getSinkType() {
        return "DB";
    }

    @Override
    public Map<String, Object> transform(RecordModel record) throws Exception {
        Map<String, Object> cqlMap = new LinkedHashMap<>();
        cqlMap.put("id", record.getId());
        cqlMap.put("record_number", record.getRecordNumber());
        
        Map<String, String> payloadMap = new LinkedHashMap<>();
        if (record.getData() != null) {
            record.getData().forEach((k, v) -> payloadMap.put(k, String.valueOf(v)));
        }
        cqlMap.put("payload", payloadMap);
        
        return cqlMap;
    }
}
