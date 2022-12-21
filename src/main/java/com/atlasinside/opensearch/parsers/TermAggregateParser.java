package com.atlasinside.opensearch.parsers;

import com.atlasinside.opensearch.types.Aggregation;
import org.opensearch.client.opensearch._types.aggregations.Aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TermAggregateParser {
    private static final String CLASSNAME = "TermAggregateParser";

    public static List<Aggregation> parse(Aggregate aggregate) {
        final String ctx = CLASSNAME + ".parse";
        try {
            if (Objects.isNull(aggregate))
                return Collections.emptyList();

            List<Aggregation> result = new ArrayList<>();
            switch (aggregate._kind()) {
                case Sterms:
                    aggregate.sterms().buckets().array()
                            .forEach(bucket -> result.add(new Aggregation(bucket.key(),
                                    bucket.docCount(), bucket.aggregations())));
                    break;
                case Lterms:
                    aggregate.lterms().buckets().array()
                            .forEach(bucket -> result.add(new Aggregation(bucket.key(),
                                    bucket.docCount(), bucket.aggregations())));
                    break;
                case Dterms:
                    aggregate.dterms().buckets().array()
                            .forEach(bucket -> result.add(new Aggregation(bucket.keyAsString(),
                                    bucket.docCount(), bucket.aggregations())));
                    break;
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }
}
