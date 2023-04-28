package com.atlasinside.opensearch.types;

import java.util.List;
import java.util.Locale;

public class ClusterResume {
    private Float diskTotal;
    private Float diskUsed;
    private Float ramMax;
    private Float ramCurrent;
    private Float heapMax;
    private Float heapCurrent;
    private final List<ElasticNode> nodes;

        public ClusterResume(List<ElasticNode> nodes) {
            this.nodes = nodes;
            getDiskTotal();
            getDiskUsed();
            getRamMax();
            getRamCurrent();
            getHeapMax();
            getHeapCurrent();
        }

    public Float getDiskTotal() {
        this.diskTotal = nodes.stream().map(ElasticNode::getDiskTotal).reduce((float) 0, Float::sum);
        return Float.parseFloat(String.format(Locale.US, "%.2f", this.diskTotal));
    }

    public Float getDiskUsed() {
        this.diskUsed = nodes.stream().map(ElasticNode::getDiskUsed).reduce((float) 0, Float::sum);
        return Float.parseFloat(String.format(Locale.US, "%.2f", this.diskUsed));
    }

    public Float getDiskAvailable() {
        return nodes.stream().map(ElasticNode::getDiskAvailable).reduce((float) 0, Float::sum);
    }

    public Float getDiskUsedPercent() {
        return Float.parseFloat(String.format(Locale.US, "%.2f", (diskUsed / diskTotal * 100)));
    }

    public Float getRamMax() {
        this.ramMax = nodes.stream().map(ElasticNode::getRamMax).reduce((float) 0, Float::sum);
        return Float.parseFloat(String.format(Locale.US, "%.2f", this.ramMax));
    }

    public Float getRamCurrent() {
        this.ramCurrent = nodes.stream().map(ElasticNode::getRamCurrent).reduce((float) 0, Float::sum);
        return Float.parseFloat(String.format(Locale.US, "%.2f", this.ramCurrent));
    }

    public Float getRamPercent() {
        return Float.parseFloat(String.format(Locale.US, "%.2f", (ramCurrent / ramMax * 100)));
    }

    public Float getCpuPercent() {
        return nodes.stream().map(ElasticNode::getCpuPercent).reduce((float) 0, Float::sum) / nodes.size();
    }

    public Float getHeapMax() {
        heapMax = nodes.stream().map(ElasticNode::getHeapMax).reduce((float) 0, Float::sum) / nodes.size();
        return Float.parseFloat(String.format(Locale.US, "%.2f", this.heapMax));
    }

    public Float getHeapCurrent() {
        heapCurrent = nodes.stream().map(ElasticNode::getHeapCurrent).reduce((float) 0, Float::sum) / nodes.size();
        return Float.parseFloat(String.format(Locale.US, "%.2f", this.heapCurrent));
    }

    public Float getHeapPercent() {
        return Float.parseFloat(String.format(Locale.US, "%.2f", (heapCurrent / heapMax * 100)));
    }
}
