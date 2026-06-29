package com.pipeline.fanout_engine.transformer;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class TransformerFactory {

    private final Map<String, RecordTransformer> transformers;

    public TransformerFactory(List<RecordTransformer> transformerList) {
        this.transformers = transformerList.stream()
                .collect(Collectors.toMap(
                        transformer -> transformer.getSinkType().toUpperCase(),
                        Function.identity()
                ));
    }

    public Optional<RecordTransformer> getTransformer(String sinkType) {
        if (sinkType == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(transformers.get(sinkType.toUpperCase()));
    }
}
