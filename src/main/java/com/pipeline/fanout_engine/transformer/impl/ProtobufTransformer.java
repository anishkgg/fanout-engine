package com.pipeline.fanout_engine.transformer.impl;

import com.pipeline.fanout_engine.model.RecordModel;
import com.pipeline.fanout_engine.transformer.RecordTransformer;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
public class ProtobufTransformer implements RecordTransformer {

    @Override
    public String getSinkType() {
        return "GRPC";
    }

    @Override
    public byte[] transform(RecordModel record) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Serialize record ID (Tag 1, wire type 2 = length-delimited)
        writeStringField(dos, 1, record.getId());

        // Serialize record number (Tag 2, wire type 0 = varint)
        writeVarintField(dos, 2, record.getRecordNumber());

        // Sort data keys to ensure deterministic ordering/tags
        Map<String, Object> data = record.getData();
        if (data != null) {
            List<String> sortedKeys = new ArrayList<>(data.keySet());
            Collections.sort(sortedKeys);
            
            for (int i = 0; i < sortedKeys.size(); i++) {
                String key = sortedKeys.get(i);
                Object val = data.get(key);
                int tag = i + 3; // Tag starts at 3 for data fields

                if (val instanceof Number) {
                    writeVarintField(dos, tag, ((Number) val).longValue());
                } else {
                    writeStringField(dos, tag, String.valueOf(val));
                }
            }
        }

        dos.flush();
        return baos.toByteArray();
    }

    private void writeStringField(DataOutputStream dos, int tag, String val) throws Exception {
        if (val == null) return;
        byte[] bytes = val.getBytes(StandardCharsets.UTF_8);
        int key = (tag << 3) | 2; // wire type 2 for length-delimited
        writeVarint(dos, key);
        writeVarint(dos, bytes.length);
        dos.write(bytes);
    }

    private void writeVarintField(DataOutputStream dos, int tag, long val) throws Exception {
        int key = (tag << 3) | 0; // wire type 0 for varint
        writeVarint(dos, key);
        writeVarint(dos, val);
    }

    private void writeVarint(DataOutputStream dos, long val) throws Exception {
        while (true) {
            if ((val & ~0x7FL) == 0) {
                dos.writeByte((int) val);
                return;
            } else {
                dos.writeByte((int) ((val & 0x7F) | 0x80));
                val >>>= 7;
            }
        }
    }
}
