package com.atlasinside.opensearch.types;

import com.atlasinside.opensearch.enums.IndexSortableProperty;
import org.opensearch.client.opensearch._types.SortOrder;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexSort {
    private Map<IndexSortableProperty, SortOrder> orders;
    private final boolean sorted;

    private IndexSort(Map<IndexSortableProperty, SortOrder> orders) {
        this.orders = orders;
        this.sorted = true;
    }

    private IndexSort(boolean sorted) {
        this.sorted = sorted;
    }

    public static IndexSort unSorted() {
        return new IndexSort(false);
    }

    public Map<IndexSortableProperty, SortOrder> getOrders() {
        return orders;
    }

    public boolean isSorted() {
        return sorted;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        if (CollectionUtils.isEmpty(orders))
            return "";
        return orders.entrySet().stream()
                .map(e -> e.getKey() + ":" + e.getValue().jsonValue())
                .collect(Collectors.joining(","));
    }

    public static class Builder {
        private final Map<IndexSortableProperty, SortOrder> orders = new HashMap<>();

        public Builder with(IndexSortableProperty property, SortOrder direction) {
            this.orders.put(property, direction);
            return this;
        }

        public IndexSort build() {
            if (CollectionUtils.isEmpty(orders))
                throw new RuntimeException("You need to define at least one property to sort");
            return new IndexSort(orders);
        }
    }
}
