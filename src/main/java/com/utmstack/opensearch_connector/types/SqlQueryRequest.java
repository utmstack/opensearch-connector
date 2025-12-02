package com.utmstack.opensearch_connector.types;

public class SqlQueryRequest {
    private String query;
    private Integer fetchSize;

    public SqlQueryRequest() {
    }

    public SqlQueryRequest(String query, Integer fetchSize) {
        this.query = query;
        this.fetchSize = fetchSize;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }
}
