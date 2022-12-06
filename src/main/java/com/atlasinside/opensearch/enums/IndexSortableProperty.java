package com.atlasinside.opensearch.enums;

public enum IndexSortableProperty {
    Index("index"),
    DocsCount("docs.count"),
    Health("health"),
    StoreSize("store.size"),
    Status("status"),
    CreationDate("creation.date.string");

    IndexSortableProperty(String jsonValue) {
        this.jsonValue = jsonValue;
    }

    private final String jsonValue;

    public String getJsonValue() {
        return jsonValue;
    }

    public static IndexSortableProperty fromJsonValue(String jsonValue) {
        switch (jsonValue) {
            case "index":
                return Index;
            case "docs.count":
                return DocsCount;
            case "health":
                return Health;
            case "store.size":
                return StoreSize;
            case "status":
                return Status;
            case "creation.date.string":
                return CreationDate;
            default:
                throw new RuntimeException(String.format("Sortable property %1$s unrecognized", jsonValue));
        }
    }
}
