package com.atlasinside.opensearch.enums;

public enum IndexSortableProperty {
    index("index"),
    docsCount("docs.count"),
    health("health"),
    storeSize("store.size"),
    status("status"),
    creationDate("creation.date.string");

    IndexSortableProperty(String jsonValue) {
        this.jsonValue = jsonValue;
    }

    private final String jsonValue;

    public String getJsonValue() {
        return jsonValue;
    }
}
