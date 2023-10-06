package com.utmstack.opensearch_connector.types;

import org.opensearch.client.opensearch._types.aggregations.Aggregate;

import java.util.Map;

public class BucketAggregation {
    private String key;
    private Long docCount;
    private Map<String, Aggregate> subAggregations;

    public BucketAggregation(String key, Long docCount, Map<String, Aggregate> subAggregations) {
        this.key = key;
        this.docCount = docCount;
        this.subAggregations = subAggregations;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getDocCount() {
        return docCount;
    }

    public void setDocCount(Long docCount) {
        this.docCount = docCount;
    }

    public Map<String, Aggregate> getSubAggregations() {
        return subAggregations;
    }

    public void setSubAggregations(Map<String, Aggregate> subAggregations) {
        this.subAggregations = subAggregations;
    }
}
