package com.atlasinside.opensearch.parsers;

import org.opensearch.client.opensearch._types.aggregations.Aggregate;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class TermAggregateParser {
    private static final String CLASSNAME = "TermAggregateParser";

    public static Map<String, Long> parse(Aggregate aggregate) {
        final String ctx = CLASSNAME + ".parse";
        try {
            if (Objects.isNull(aggregate))
                return Collections.emptyMap();

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
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }
}
