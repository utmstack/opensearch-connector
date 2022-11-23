package com.atlasinside.opensearch;

import com.atlasinside.opensearch.enums.HttpScheme;
import com.atlasinside.opensearch.exceptions.OpenSearchException;
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
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.UpdateByQueryResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OpenSearch {
    private static final String CLASSNAME = "OpenSearch";
    private final OpenSearchClient client;

    private OpenSearch(OpenSearchClient client) {
        this.client = client;
    }

    /**
     * Perform a search operation
     *
     * @param query        Query to be executed
     * @param aggs         Aggregations to be performed
     * @param index        Index were the search will be performed, you can use a pattern too
     * @param size         Amount of hits you want to return
     * @param responseType Type of the object that will be mapped in the response
     * @return A {@link SearchResponse} object with the results of the performed operation
     * @throws OpenSearchException In case of any error
     */
    public <T> SearchResponse<T> search(@Nullable Query query,
                                        @Nullable Map<String, Aggregation> aggs,
                                        String index,
                                        int size,
                                        Class<T> responseType) throws OpenSearchException {
        final String ctx = CLASSNAME + ".search";
        try {
            return client.search(s -> s
                    .index(index)
                    .query(query)
                    .aggregations(aggs)
                    .size(size), responseType);
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Perform an update by query operation
     *
     * @param query  Query to be executed
     * @param index  Index were the update will be performed, you can use a pattern too
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
     * @param index    Index were the index will be performed, you can use a pattern too
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
     * @param index Index were the index will be performed, you can use a pattern too
     * @return True if index exist, false otherwise
     * @throws OpenSearchException In case of any error
     */
    public boolean indexExist(String index) throws OpenSearchException {
        final String ctx = CLASSNAME + ".indexExist";
        try {
            return client.indices().exists(e -> e.index(index)).value();
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
