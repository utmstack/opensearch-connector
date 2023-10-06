package com.utmstack.opensearch_connector.types;

import com.google.gson.annotations.SerializedName;

public class Index {
    private String health;
    private String status;
    private String index;
    @SerializedName("docs.count")
    private Long docsCount;
    @SerializedName("store.size")
    private String size;
    @SerializedName("creation.date.string")
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

    public Long getDocsCount() {
        return docsCount;
    }


    public void setDocsCount(Long docsCount) {
        this.docsCount = docsCount;
    }

    public String getSize() {
        return size;
    }


    public void setSize(String size) {
        this.size = size;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }
}
