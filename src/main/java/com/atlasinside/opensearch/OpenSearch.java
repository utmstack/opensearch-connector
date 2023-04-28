package com.atlasinside.opensearch;

import com.atlasinside.opensearch.clients.OpensearchClient;
import com.atlasinside.opensearch.clients.RestClient;
import com.atlasinside.opensearch.enums.HttpScheme;
import com.atlasinside.opensearch.enums.TermOrder;
import com.atlasinside.opensearch.exceptions.OpenSearchException;
import com.atlasinside.opensearch.parsers.TermAggregateParser;
import com.atlasinside.opensearch.types.BucketAggregation;
import com.atlasinside.opensearch.types.ElasticCluster;
import com.atlasinside.opensearch.types.Index;
import com.atlasinside.opensearch.types.IndexSort;
import com.atlasinside.opensearch.util.IndexUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.*;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.cat.IndicesRequest;
import org.opensearch.client.opensearch.cat.NodesRequest;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.client.opensearch.cat.nodes.NodesRecord;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.UpdateByQueryResponse;
import org.opensearch.client.opensearch.indices.get_mapping.IndexMappingRecord;

import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class OpenSearch {
    private static final String CLASSNAME = "OpenSearch";
    private final OpenSearchClient client;
    private RestClient restClient;

    private OpenSearch(OpenSearchClient client) {
        this.client = client;
    }

    private OpenSearch(OpenSearchClient client, RestClient restClient) {
        this.client = client;
        this.restClient = restClient;
    }

    /**
     * Perform a search operation
     *
     * @param request      Search request definition
     * @param responseType Type of the object that will be mapped in the response
     * @return A {@link SearchResponse} object with the results of the performed operation
     * @throws OpenSearchException In case of any error
     */
    public <T> SearchResponse<T> search(SearchRequest request, Class<T> responseType) throws OpenSearchException {
        final String ctx = CLASSNAME + ".search";
        try {
            return client.search(request, responseType);
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Perform an update by query operation
     *
     * @param query  Query to be executed
     * @param index  Index where the update will be performed, you can use a pattern too
     * @param script Script that perform the update
     * @return A {@link UpdateByQueryResponse} object with the results of the performed operation
     * @throws OpenSearchException In case of any error
     */
    public UpdateByQueryResponse updateByQuery(Query query, String index, String script)
            throws OpenSearchException {
        final String ctx = CLASSNAME + ".updateByQuery";
        try {
            return client.updateByQuery(u -> u
                    .index(index)
                    .query(query)
                    .script(new Script.Builder()
                            .inline(new InlineScript.Builder()
                                    .lang("painless")
                                    .source(script)
                                    .build())
                            .build())
                    .refresh(true));
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Perform an index operation
     *
     * @param index    Index where the index will be performed, you can use a pattern too
     * @param document Information that will be indexed
     * @return A {@link IndexResponse} object with the results of the performed operation
     * @throws OpenSearchException In case of any error
     */
    public <T> IndexResponse index(String index, T document) throws OpenSearchException {
        final String ctx = CLASSNAME + ".index";
        try {
            return client.index(i -> i
                    .index(index)
                    .refresh(Refresh.True)
                    .document(document));
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Check if some index exist
     *
     * @param index Index where the indexing will be performed, you can use a pattern too
     * @return True if index exist, false otherwise
     * @throws OpenSearchException In case of any error
     */
    public boolean indexExist(String index) throws OpenSearchException {
        final String ctx = CLASSNAME + ".indexExist";
        try {
            return !CollectionUtils.isEmpty(client.indices()
                    .resolveIndex(e -> e.name(index)).indices());
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }


    /**
     * Removes one or more indices
     *
     * @param indices The list of indices to delete
     * @throws OpenSearchException In case of any error
     */
    public void deleteIndex(List<String> indices) throws OpenSearchException {
        final String ctx = CLASSNAME + ".deleteIndex";
        try {
            client.indices().delete(d -> d.index(indices));
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Search for the possible values of the field in the specified index or index pattern.
     * If the field is a text, then you need to use it as a keyword.
     * <br>
     * Example:
     * <br>
     * If you field {name} is a text then you need to pass the field as {name.keyword}
     *
     * @param field     Field to search for his values
     * @param index     Index where the action will be performed, you can use a pattern too
     * @param query     Any query to perform before get the field values
     * @param top       Top values you want to get
     * @param termOrder There are to ways to order the results, alphabetically (by the values name)
     *                  and metrically (by the amount of documents)
     * @param sortOrder The way that you want to sort the results Asc or Desc
     * @return A map with all values founded for the field and the amount of documents for each value
     * @throws OpenSearchException In case of any error
     */
    public Map<String, Long> getFieldValues(String field, String index, Query query, Integer top,
                                            TermOrder termOrder, SortOrder sortOrder) throws OpenSearchException {
        final String ctx = CLASSNAME + ".getFieldValues";
        try {
            final String AGG_NAME = "field_values";
            Map<String, SortOrder> order = Map.of(termOrder.jsonValue(), sortOrder);
            org.opensearch.client.opensearch._types.aggregations.Aggregation fieldValuesAgg = org.opensearch.client.opensearch._types.aggregations.Aggregation.of(agg -> agg.terms(t -> t.field(field)
                    .size(top != null ? top : 5).order(List.of(order))));
            SearchResponse<Object> response = client.search(s -> s
                    .query(query).size(0)
                    .index(index)
                    .aggregations(Map.of(AGG_NAME, fieldValuesAgg)), Object.class);

            List<BucketAggregation> list = TermAggregateParser.parse(response.aggregations().get(AGG_NAME));
            if (CollectionUtils.isEmpty(list))
                return Collections.emptyMap();
            return list.stream().collect(Collectors.toMap(BucketAggregation::getKey, BucketAggregation::getDocCount));
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Gets all fields of an index
     *
     * @param index Index or pattern from which fields will be extracted
     * @return A map with the name of a field as the key and type of field as the value
     * @throws OpenSearchException In case of any error
     */
    public Map<String, String> getIndexProperties(String index) throws OpenSearchException {
        final String ctx = CLASSNAME + ".getIndexProperties";
        try {
            Map<String, IndexMappingRecord> mapping = client.indices().getMapping(f -> f.index(index)).result();

            if (MapUtils.isEmpty(mapping))
                return Collections.emptyMap();

            Map<String, String> result = new TreeMap<>();
            mapping.forEach((k, v) -> IndexUtils.propertiesFromMapping(v.mappings().properties(), result, null));

            return result;
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Gets a list with the index information that match with the pattern
     *
     * @param pattern   The pattern or the index name from which you want to get the information
     * @param indexSort Set of properties to sort the result
     * @return A list of ${@link Index}
     * @throws OpenSearchException In case of any error
     */
    public List<IndicesRecord> getIndices(String pattern, IndexSort indexSort) throws OpenSearchException {
        final String ctx = CLASSNAME + ".getIndices";
        try {
            if (Objects.isNull(indexSort))
                indexSort = IndexSort.unSorted();

            if (StringUtils.isEmpty(pattern))
                pattern = "*";

            final String headers = "index,docs.count,health,store.size,status,creation.date.string";
            IndicesRequest.Builder rq = new IndicesRequest.Builder();
            rq.index(pattern);
            rq.headers(headers);
            rq.sort(indexSort.toString());

            return client.cat().indices(rq.build()).valueBody();
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns information about a cluster’s nodes
     *
     * @return A list of ${@link NodesRecord} with the status information for each node of the cluster
     * @throws OpenSearchException In case of any error
     */
    public Optional<ElasticCluster> getClusterNodesInfo() throws OpenSearchException {
        final String ctx = CLASSNAME + ".getNodes";
        try {
            final String headers = "master,ip,disk.total,disk.used,disk.used_percent,disk.avail,name,ram.percent,ram.current,ram.max,cpu,heap.current,heap.percent,heap.max";
            NodesRequest.Builder rq = new NodesRequest.Builder();
            rq.headers(headers);
            rq.bytes(Bytes.MegaBytes);

            List<NodesRecord> nodes = client.cat().nodes(rq.build()).valueBody();
            if (CollectionUtils.isEmpty(nodes))
                return Optional.empty();

            return Optional.of(new ElasticCluster(nodes));
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String user;
        private String password;
        private HttpHost host;

        public Builder withCredentials(String user, String password) {
            this.user = user;
            this.password = password;
            return this;
        }

        public Builder withHost(String hostname, int port, HttpScheme scheme) {
            host = new HttpHost(hostname, port, scheme.toString());
            return this;
        }

        public OpenSearch build() {
            final String ctx = CLASSNAME + ".build";
            try {
                return new OpenSearch(OpensearchClient.build(user, password, host),
                        new RestClient(user, password, host));
            } catch (Exception e) {
                throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
            }
        }
    }
}
