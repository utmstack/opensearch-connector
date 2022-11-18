package com.atlasinside.opensearch;

import com.atlasinside.opensearch.types.enums.HttpHostScheme;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.InlineScript;
import org.opensearch.client.opensearch._types.Script;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.UpdateByQueryRequest;
import org.opensearch.client.opensearch.core.UpdateByQueryResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import java.util.ArrayList;
import java.util.List;
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
     * @param index        Index were the search will be performed, you can use a pattern too
     * @param responseType Type of the object that will be mapped in the response
     * @return A {@link SearchResponse} object with the results of the performed search
     */
    public <T> SearchResponse<T> search(Query query, String index, Class<T> responseType) {
        final String ctx = CLASSNAME + ".search";
        try {
            return client.search(new SearchRequest.Builder()
                    .index(index)
                    .query(query)
                    .build(), responseType);
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Perform an update by query operation
     *
     * @param query  Query to be executed
     * @param index  Index were the search will be performed, you can use a pattern too
     * @param script Script that perform the update
     */
    public UpdateByQueryResponse updateByQuery(Query query, String index, String script) {
        final String ctx = CLASSNAME + ".updateByQuery";
        try {
            return client.updateByQuery(new UpdateByQueryRequest.Builder()
                    .index(index)
                    .query(query)
                    .script(new Script.Builder()
                            .inline(new InlineScript.Builder()
                                    .lang("painless")
                                    .source(script)
                                    .build())
                            .build())
                    .refresh(true)
                    .build());
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
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

        public Builder withHost(String hostname, int port, HttpHostScheme scheme) {
            hosts.add(new HttpHost(hostname, port, scheme.toString()));
            return this;
        }

        public OpenSearch build() {
            try {
                Objects.requireNonNull(credentialsProvider, "No credentials were provided");
                if (hosts.isEmpty())
                    throw new RuntimeException("No hosts definition were provided");

                RestClient restClient = RestClient.builder(hosts.toArray(new HttpHost[0])).
                        setHttpClientConfigCallback(builder -> builder.setDefaultCredentialsProvider(credentialsProvider))
                        .build();

                OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

                return new OpenSearch(new OpenSearchClient(transport));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
