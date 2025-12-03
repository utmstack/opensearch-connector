package com.utmstack.opensearch_connector.types;

import java.util.List;

public class SqlQueryResponse {
    private List<SqlColumn> schema;
    private List<List<Object>> datarows;
    private Integer total;
    private Integer size;
    private Integer status;

    public SqlQueryResponse() {
    }

    public SqlQueryResponse(List<SqlColumn> schema, List<List<Object>> datarows, Integer total, Integer size, Integer status) {
        this.schema = schema;
        this.datarows = datarows;
        this.total = total;
        this.size = size;
        this.status = status;
    }

    public List<SqlColumn> getSchema() {
        return schema;
    }

    public void setSchema(List<SqlColumn> schema) {
        this.schema = schema;
    }

    public List<List<Object>> getDatarows() {
        return datarows;
    }

    public void setDatarows(List<List<Object>> datarows) {
        this.datarows = datarows;
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

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}

