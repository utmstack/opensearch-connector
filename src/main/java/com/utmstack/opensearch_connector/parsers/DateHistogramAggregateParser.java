package com.utmstack.opensearch_connector.parsers;

import com.utmstack.opensearch_connector.types.BucketAggregation;
import org.opensearch.client.opensearch._types.aggregations.Aggregate;
import org.opensearch.client.opensearch._types.aggregations.DateHistogramBucket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DateHistogramAggregateParser {
    private static final String CLASSNAME = "DateHistogramAggregateParser";

    public static List<BucketAggregation> parse(Aggregate aggregate) {
        final String ctx = CLASSNAME + ".parse";
        try {
            if (Objects.isNull(aggregate))
                return Collections.emptyList();

            List<DateHistogramBucket> buckets = aggregate.dateHistogram().buckets().array();

            if (Objects.isNull(buckets))
                return Collections.emptyList();

            List<BucketAggregation> result = new ArrayList<>();

            buckets.forEach(bucket -> result.add(new BucketAggregation(bucket.keyAsString(),
                    bucket.docCount(), bucket.aggregations())));

            return result;
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }
}
