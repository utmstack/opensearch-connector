package com.atlasinside.opensearch.util;

import org.opensearch.client.opensearch._types.aggregations.Aggregate;

import java.util.LinkedHashMap;
import java.util.Map;

public class TermAggregateParser {
    public static Map<String, Long> parse(Aggregate aggregate) {
        Map<String, Long> result = new LinkedHashMap<>();
        switch (aggregate._kind()) {
            case Sterms:
                aggregate.sterms().buckets().array()
                        .forEach(bucket -> result.put(bucket.key(), bucket.docCount()));
                break;
            case Lterms:
                aggregate.lterms().buckets().array()
                        .forEach(bucket -> result.put(bucket.key(), bucket.docCount()));
                break;
            case Dterms:
                aggregate.dterms().buckets().array()
                        .forEach(bucket -> result.put(bucket.keyAsString(), bucket.docCount()));
                break;
        }
        return result;
    }
}
