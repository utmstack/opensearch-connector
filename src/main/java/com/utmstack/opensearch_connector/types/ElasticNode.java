package com.utmstack.opensearch_connector.types;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.opensearch.cat.nodes.NodesRecord;

public class ElasticNode {
    private final String ip;
    private final String name;
    private final Float cpuPercent;
    private final Float diskTotal;
    private final Float diskUsed;
    private final Float diskUsedPercent;
    private final Float diskAvailable;
    private final Float ramMax;
    private final Float ramCurrent;
    private final Float ramPercent;
    private final Float heapCurrent;
    private final Float heapMax;
    private final Float heapPercent;

    public ElasticNode(NodesRecord node) {
        this.ip = node.ip();
        this.name = node.name();
        this.cpuPercent = !StringUtils.isBlank(node.cpu()) ? Float.parseFloat(node.cpu()) / 1024 : 0F;
        this.diskTotal = !StringUtils.isBlank(node.diskTotal()) ? Float.parseFloat(node.diskTotal()) / 1024 : 0F;
        this.diskUsed = !StringUtils.isBlank(node.diskUsed()) ? Float.parseFloat(node.diskUsed()) / 1024 : 0F;
        this.diskUsedPercent = !StringUtils.isBlank(node.diskUsedPercent()) ? Float.parseFloat(node.diskUsedPercent()) : 0F;
        this.diskAvailable = !StringUtils.isBlank(node.diskAvail()) ? Float.parseFloat(node.diskAvail()) / 1024 : 0F;
        this.ramMax = !StringUtils.isBlank(node.ramMax()) ? Float.parseFloat(node.ramMax()) / 1024 : 0F;
        this.ramCurrent = !StringUtils.isBlank(node.ramCurrent()) ? Float.parseFloat(node.ramCurrent()) / 1024 : 0F;
        this.ramPercent = !StringUtils.isBlank(node.ramPercent()) ? Float.parseFloat(node.ramPercent()) : 0F;
        this.heapCurrent = !StringUtils.isBlank(node.heapCurrent()) ? Float.parseFloat(node.heapCurrent()) / 1024 : 0F;
        this.heapMax = !StringUtils.isBlank(node.heapMax()) ? Float.parseFloat(node.heapMax()) / 1024 : 0F;
        this.heapPercent = !StringUtils.isBlank(node.heapPercent()) ? Float.parseFloat(node.heapPercent()) : 0F;
    }

    public String getIp() {
        return ip;
    }

    public String getName() {
        return name;
    }

    public Float getCpuPercent() {
        return cpuPercent;
    }

    public Float getDiskTotal() {
        return diskTotal;
    }

    public Float getDiskUsed() {
        return diskUsed;
    }

    public Float getDiskUsedPercent() {
        return diskUsedPercent;
    }

    public Float getDiskAvailable() {
        return diskAvailable;
    }

    public Float getRamMax() {
        return ramMax;
    }

    public Float getRamCurrent() {
        return ramCurrent;
    }

    public Float getRamPercent() {
        return ramPercent;
    }

    public Float getHeapCurrent() {
        return heapCurrent;
    }

    public Float getHeapMax() {
        return heapMax;
    }

    public Float getHeapPercent() {
        return heapPercent;
    }
}
