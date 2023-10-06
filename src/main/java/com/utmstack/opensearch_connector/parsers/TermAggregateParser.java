package com.utmstack.opensearch_connector.parsers;

import com.utmstack.opensearch_connector.types.BucketAggregation;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.opensearch._types.aggregations.Aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TermAggregateParser {
    private static final String CLASSNAME = "TermAggregateParser";

    public static List<BucketAggregation> parse(Aggregate aggregate) {
        final String ctx = CLASSNAME + ".parse";
        try {
            if (Objects.isNull(aggregate))
                return Collections.emptyList();

            List<BucketAggregation> result = new ArrayList<>();
            switch (aggregate._kind()) {
                case Sterms:
                    aggregate.sterms().buckets().array()
                            .forEach(bucket -> result.add(new BucketAggregation(bucket.key(),
                                    bucket.docCount(), bucket.aggregations())));
                    break;
                case Lterms:
                    aggregate.lterms().buckets().array()
                            .forEach(bucket -> {
                                String key = StringUtils.isEmpty(bucket.keyAsString()) ? bucket.key() : bucket.keyAsString();
                                result.add(new BucketAggregation(key, bucket.docCount(), bucket.aggregations()));
                            });
                    break;
                case Dterms:
                    aggregate.dterms().buckets().array()
                            .forEach(bucket -> {
                                String key = StringUtils.isEmpty(bucket.keyAsString()) ? String.valueOf(bucket.key()) : bucket.keyAsString();
                                result.add(new BucketAggregation(key, bucket.docCount(), bucket.aggregations()));
                            });
                    break;
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }
}
