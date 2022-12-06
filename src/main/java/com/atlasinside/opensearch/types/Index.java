package com.atlasinside.opensearch.types;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

public class Index {
    private String health;
    private String status;
    private String index;
    private Long docsCount;
    private String size;
    private String creationDate;



    public String getHealth() {
        return health;
    }

    public void setHealth(String health) {
        this.health = health;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @JsonGetter("docsCount")
    public Long getDocsCount() {
        return docsCount;
    }

    @JsonSetter("docs.count")
    public void setDocsCount(Long docsCount) {
        this.docsCount = docsCount;
    }

    @JsonGetter("size")
    public String getSize() {
        return size;
    }

    @JsonSetter("store.size")
    public void setSize(String size) {
        this.size = size;
    }

    @JsonGetter("creationDate")
    public String getCreationDate() {
        return creationDate;
    }

    @JsonSetter("creation.date.string")
    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }
}
