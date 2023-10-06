package com.utmstack.opensearch_connector.enums;

public enum TermOrder {
    Key("_key"),
    Count("_count");

    private final String jsonValue;

    TermOrder(String jsonValue) {
        this.jsonValue = jsonValue;
    }

    public String jsonValue() {
        return this.jsonValue;
    }
}
