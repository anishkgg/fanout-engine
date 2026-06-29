package com.pipeline.fanout_engine.transformer;

import com.pipeline.fanout_engine.model.RecordModel;

public interface RecordTransformer {
    String getSinkType();
    Object transform(RecordModel record) throws Exception;
}
