package com.pipeline.fanout_engine.transformer.impl;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.pipeline.fanout_engine.model.RecordModel;
import com.pipeline.fanout_engine.transformer.RecordTransformer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class XmlTransformer implements RecordTransformer {

    private final XmlMapper xmlMapper;

    @Override
    public String getSinkType() {
        return "MESSAGE_QUEUE";
    }

    @Override
    public String transform(RecordModel record) throws Exception {
        // Wrap raw data under a root <record> element
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("id", record.getId());
        root.put("recordNumber", record.getRecordNumber());
        root.put("data", record.getData());
        return xmlMapper.writer().withRootName("record").writeValueAsString(root);
    }
}
