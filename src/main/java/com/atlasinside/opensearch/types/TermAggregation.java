package com.atlasinside.opensearch.types;

import org.opensearch.client.opensearch._types.aggregations.Aggregate;

import java.util.Map;

public class TermAggregation {
    private String key;
    private Long value;
    private Map<String, Aggregate> subAggregations;

    public TermAggregation(String key, Long value, Map<String, Aggregate> subAggregations) {
        this.key = key;
        this.value = value;
        this.subAggregations = subAggregations;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public Map<String, Aggregate> getSubAggregations() {
        return subAggregations;
    }

    public void setSubAggregations(Map<String, Aggregate> subAggregations) {
        this.subAggregations = subAggregations;
    }
}
