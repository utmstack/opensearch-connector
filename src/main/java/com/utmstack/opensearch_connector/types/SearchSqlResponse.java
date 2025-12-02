package com.utmstack.opensearch_connector.types;

import java.util.List;

public class SearchSqlResponse<T> {
    private List<T> data;
    private Integer total;
    private Integer size;

    public SearchSqlResponse() {
    }

    public SearchSqlResponse(List<T> data, Integer total, Integer size) {
        this.data = data;
        this.total = total;
        this.size = size;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }
}

