package com.atlasinside.opensearch;

import com.atlasinside.opensearch.enums.HttpScheme;
import com.atlasinside.opensearch.enums.TermOrder;
import com.atlasinside.opensearch.exceptions.OpenSearchException;
import com.atlasinside.opensearch.parsers.TermAggregateParser;
import com.atlasinside.opensearch.types.IndexPropertyType;
import com.atlasinside.opensearch.util.IndexUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.InlineScript;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.Script;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.UpdateByQueryResponse;
import org.opensearch.client.opensearch.indices.get_mapping.IndexMappingRecord;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.*;

public class OpenSearch {
    private static final String CLASSNAME = "OpenSearch";
    private final OpenSearchClient client;

    private OpenSearch(OpenSearchClient client) {
        this.client = client;
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
        } catch (IOException e) {
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
        } catch (IOException e) {
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
        } catch (IOException e) {
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
     * @param sortOrder The way that you want to sort the resuts Asc or Desc
     * @return A map with all values founded for the field and the amount of documents for each value
     * @throws OpenSearchException In case of any error
     */
    public Map<String, Long> getFieldValues(String field, String index, Query query, Integer top,
                                            TermOrder termOrder, SortOrder sortOrder) throws OpenSearchException {
        final String ctx = CLASSNAME + ".getFieldValues";
        try {
            Validate.notBlank(field, "The Field parameter must not be null or empty");
            Validate.notBlank(index, "The Index parameter must not be null or empty");

            final String AGG_NAME = "field_values";
            Map<String, SortOrder> order = Map.of(termOrder.jsonValue(), sortOrder);
            Aggregation fieldValuesAgg = Aggregation.of(agg -> agg.terms(t -> t.field(field)
                    .size(top != null ? top : 5).order(List.of(order))));
            SearchResponse<Object> response = client.search(s -> s
                    .query(query).size(0)
                    .index(index)
                    .aggregations(Map.of(AGG_NAME, fieldValuesAgg)), Object.class);

            return TermAggregateParser.parse(response.aggregations().get(AGG_NAME));
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Gets all fields of an index
     *
     * @param index Index or pattern from which fields will be extracted
     * @return A map with the name of a field as the key and type of a field as the value
     * @throws OpenSearchException In case of any error
     */
    public Map<String, String> getIndexProperties(String index) throws OpenSearchException {
        final String ctx = CLASSNAME + ".getIndexProperties";
        try {
            Validate.notBlank(index, "The Index parameter must not be null or empty");
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


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private CredentialsProvider credentialsProvider;
        private final List<HttpHost> hosts = new ArrayList<>();

        public Builder withCredentials(String user, String password) {
            if (credentialsProvider != null)
                return this;
            credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
            return this;
        }

        public Builder withHost(String hostname, int port, HttpScheme scheme) {
            hosts.add(new HttpHost(hostname, port, scheme.toString()));
            return this;
        }

        public Builder withHost(String url) {
            hosts.add(HttpHost.create(url));
            return this;
        }

        public OpenSearch build() {
            try {
                Objects.requireNonNull(credentialsProvider, "No credentials were provided");
                if (hosts.isEmpty())
                    throw new RuntimeException("No hosts definition were provided");

                SSLContextBuilder sslBuilder = SSLContexts.custom()
                        .loadTrustMaterial(null, (x509Certificates, s) -> true);
                final SSLContext sslContext = sslBuilder.build();

                RestClient restClient = RestClient.builder(hosts.toArray(new HttpHost[0])).
                        setHttpClientConfigCallback(builder -> builder
                                .setSSLContext(sslContext)
                                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                .setDefaultCredentialsProvider(credentialsProvider))
                        .build();

                OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

                return new OpenSearch(new OpenSearchClient(transport));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
