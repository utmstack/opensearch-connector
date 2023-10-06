package com.utmstack.opensearch_connector.types;

import org.opensearch.client.opensearch.cat.nodes.NodesRecord;

import java.util.ArrayList;
import java.util.List;

public class ElasticCluster {
    private final ClusterResume resume;
    private final List<ElasticNode> nodes = new ArrayList<>();

    public ElasticCluster(List<NodesRecord> nodes) {
        nodes.forEach(n -> this.nodes.add(new ElasticNode(n)));
        resume = new ClusterResume(this.nodes);
    }

    public ClusterResume getResume() {
        return resume;
    }

    public List<ElasticNode> getNodes() {
        return nodes;
    }
}
