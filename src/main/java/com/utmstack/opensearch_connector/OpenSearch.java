package com.utmstack.opensearch_connector;

import com.utmstack.opensearch_connector.clients.OpensearchClient;
import com.utmstack.opensearch_connector.clients.RestClient;
import com.utmstack.opensearch_connector.enums.HttpMethod;
import com.utmstack.opensearch_connector.enums.HttpScheme;
import com.utmstack.opensearch_connector.enums.TermOrder;
import com.utmstack.opensearch_connector.exceptions.OpenSearchException;
import com.utmstack.opensearch_connector.parsers.TermAggregateParser;
import com.utmstack.opensearch_connector.types.BucketAggregation;
import com.utmstack.opensearch_connector.types.ElasticCluster;
import com.utmstack.opensearch_connector.types.IndexSort;
import com.utmstack.opensearch_connector.util.IndexUtils;
import okhttp3.Response;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.*;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
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

import java.util.*;
import java.util.stream.Collectors;

public class OpenSearch {
    private static final String CLASSNAME = "OpenSearch";
    private final OpenSearchClient client;
    private final RestClient restClient;

    private OpenSearch(OpenSearchClient client, RestClient restClient) {
        this.client = client;
        this.restClient = restClient;
    }


    /**
     * Perform a search operation and returns the results in the specified response type.
     *
     * @param request      The search request containing the query parameters.
     * @param responseType The type of object to map the search results into.
     * @return A {@link SearchResponse} containing the search results mapped to the specified type.
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
     * Performs an update-by-query operation in the OpenSearch engine with the specified query, index, and script.
     *
     * @param query  The query to filter documents for the update operation.
     * @param index  The name of the index where the documents are located.
     * @param script The painless script to be executed as part of the update operation.
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
     * Indexes a document of type T in the specified OpenSearch index.
     *
     * @param index    The name of the index where the document will be indexed.
     * @param document The document of type T to be indexed.
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
     * Checks if an OpenSearch index with the specified name exists.
     *
     * @param index The name of the index to check for existence.
     * @return True if the index exists, false otherwise.
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
     * Deletes one or more OpenSearch indices based on the given list of index names.
     *
     * @param indices A list of index names to be deleted.
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
     * If the field is of type text, then you need to use it as a keyword.
     * <br>
     * Example:
     * <br>
     * If you field <strong>{@code name}</strong> is of type text, then you need to pass the field as <strong>{@code name.keyword}</strong>
     *
     * @param field     The name of the field to retrieve values from.
     * @param index     Index where the action will be performed, you can use a pattern too
     * @param query     Any query to perform before get the field values
     * @param top       The maximum number of values to retrieve (optional, use null for default).
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
            Aggregation fieldValuesAgg = Aggregation.of(agg -> agg.terms(t -> t.field(field)
                    .size(top != null ? top : 5).order(List.of(order))));
            SearchResponse<Object> response = client.search(s -> s
                    .query(query).size(0).index(index)
                    .aggregations(Map.of(AGG_NAME, fieldValuesAgg)), Object.class);

            List<BucketAggregation> list = TermAggregateParser.parse(response.aggregations().get(AGG_NAME));
            if (CollectionUtils.isEmpty(list))
                return Collections.emptyMap();

            return list.stream().collect(Collectors.toMap(BucketAggregation::getKey, BucketAggregation::getDocCount, (a, b) -> b, LinkedHashMap::new));

        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Retrieves properties and their data types from the mapping of an index.
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
     * Retrieves a list of indices based on the provided pattern and sorting criteria.
     *
     * @param pattern   The pattern to filter indices (default is "*").
     * @param indexSort The sorting criteria for the retrieved indices (default is unsorted).
     * @return A list of ${@link IndicesRecord}
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
     * Retrieves information about the OpenSearch cluster nodes.
     *
     * @return An Optional containing an ElasticCluster object representing the cluster nodes' information,
     * or an empty Optional if no nodes are found.
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

    /**
     * You can perform a direct http request to the opensearch instance you are connected
     *
     * @param uri         The URI of the request.
     * @param queryParams A map with any query parameters needed for the request.
     * @param body        The body of the request.
     * @param method      The HTTP method to use. We just allow (GET, PUT, POST).
     *                    The body object will be ignored for GET requests
     * @return A {@link Response} object representing the HTTP response to the request.
     * @throws RuntimeException In case of any error.
     */
    public Response executeHttpRequest(String uri, Map<String, String> queryParams, Object body, HttpMethod method) {
        final String ctx = CLASSNAME + ".executeHttpRequest";
        try {
            switch (method) {
                case GET:
                    return restClient.get(uri, queryParams);
                case PUT:
                    return restClient.put(uri, queryParams, body);
                case POST:
                    return restClient.post(uri, queryParams, body);
                default:
                    throw new IllegalArgumentException("Unsupported HTTP method");
            }
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
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
