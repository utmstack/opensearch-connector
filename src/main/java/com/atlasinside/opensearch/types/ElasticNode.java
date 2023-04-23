package com.atlasinside.opensearch.types;

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
        this.cpuPercent = Float.parseFloat(node.cpu());
        this.diskTotal = Float.parseFloat(node.diskTotal()) / 1024;
        this.diskUsed = Float.parseFloat(node.diskUsed()) / 1024;
        this.diskUsedPercent = Float.parseFloat(node.diskUsedPercent());
        this.diskAvailable = Float.parseFloat(node.diskAvail()) / 1024;
        this.ramMax = Float.parseFloat(node.ramMax()) / 1024;
        this.ramCurrent = Float.parseFloat(node.ramCurrent()) / 1024;
        this.ramPercent = Float.parseFloat(node.ramPercent());
        this.heapCurrent = Float.parseFloat(node.heapCurrent()) / 1024;
        this.heapMax = Float.parseFloat(node.heapMax()) / 1024;
        this.heapPercent = Float.parseFloat(node.heapPercent());
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
