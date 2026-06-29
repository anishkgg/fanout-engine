package com.pipeline.fanout_engine.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.pipeline.fanout_engine.model.RecordModel;
import com.pipeline.fanout_engine.transformer.impl.AvroCqlTransformer;
import com.pipeline.fanout_engine.transformer.impl.JsonTransformer;
import com.pipeline.fanout_engine.transformer.impl.ProtobufTransformer;
import com.pipeline.fanout_engine.transformer.impl.XmlTransformer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RecordTransformerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final XmlMapper xmlMapper = new XmlMapper();

    private final RecordModel testRecord = RecordModel.builder()
            .id("rec-123")
            .recordNumber(42L)
            .data(Map.of("name", "Alice", "score", 95))
            .build();

    @Test
    void testJsonTransformer() throws Exception {
        JsonTransformer transformer = new JsonTransformer(objectMapper);
        assertEquals("REST", transformer.getSinkType());

        String result = transformer.transform(testRecord);
        assertTrue(result.contains("\"name\":\"Alice\""));
        assertTrue(result.contains("\"score\":95"));
    }

    @Test
    void testXmlTransformer() throws Exception {
        XmlTransformer transformer = new XmlTransformer(xmlMapper);
        assertEquals("MESSAGE_QUEUE", transformer.getSinkType());

        String result = transformer.transform(testRecord);
        assertTrue(result.startsWith("<record>"));
        assertTrue(result.contains("<id>rec-123</id>"));
        assertTrue(result.contains("<recordNumber>42</recordNumber>"));
        assertTrue(result.contains("<name>Alice</name>"));
    }

    @Test
    void testProtobufTransformer() throws Exception {
        ProtobufTransformer transformer = new ProtobufTransformer();
        assertEquals("GRPC", transformer.getSinkType());

        byte[] result = transformer.transform(testRecord);
        assertNotNull(result);
        assertTrue(result.length > 0);
        String strVal = new String(result);
        assertTrue(strVal.contains("rec-123"));
    }

    @Test
    void testAvroCqlTransformer() throws Exception {
        AvroCqlTransformer transformer = new AvroCqlTransformer();
        assertEquals("DB", transformer.getSinkType());

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformer.transform(testRecord);
        assertEquals("rec-123", result.get("id"));
        assertEquals(42L, result.get("record_number"));
        
        Map<?, ?> payload = (Map<?, ?>) result.get("payload");
        assertEquals("Alice", payload.get("name"));
        assertEquals("95", payload.get("score"));
    }
}
