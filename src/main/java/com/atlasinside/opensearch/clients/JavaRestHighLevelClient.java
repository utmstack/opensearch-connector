package com.atlasinside.opensearch.clients;

import com.atlasinside.opensearch.exceptions.OpenSearchException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.opensearch._types.Refresh;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.script.Script;
import org.opensearch.search.SearchHits;
import org.springframework.util.CollectionUtils;

import javax.net.ssl.SSLContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaRestHighLevelClient {
    private static final String CLASSNAME = "RestHighLevelClient";

    private final RestHighLevelClient client;
    private final ObjectMapper om = new ObjectMapper();

    public JavaRestHighLevelClient(String user, String password, HttpHost host) {
        final String ctx = CLASSNAME + ".RestHighLevelClient";
        try {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));

            if (Objects.isNull(host))
                throw new RuntimeException("No hosts definition were provided");

            SSLContextBuilder sslBuilder = SSLContexts.custom()
                    .loadTrustMaterial(null, (x509Certificates, s) -> true);
            final SSLContext sslContext = sslBuilder.build();

            RestClientBuilder restClientBuilder = RestClient.builder(host)
                    .setHttpClientConfigCallback(builder -> builder
                            .setSSLContext(sslContext)
                            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                            .setDefaultCredentialsProvider(credentialsProvider));

            client = new RestHighLevelClient(restClientBuilder);
            om.registerModule(new JavaTimeModule());
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    public SearchResponse search(SearchRequest request) throws OpenSearchException {
        final String ctx = CLASSNAME + ".search";
        try {
            return client.search(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    public <T> List<T> search(SearchRequest request, Class<T> type) throws OpenSearchException {
        final String ctx = CLASSNAME + ".search";
        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            SearchHits searchHits = response.getHits();
            if (searchHits.getTotalHits().value == 0)
                return Collections.emptyList();
            return Stream.of(searchHits.getHits()).map(hit -> {
                try {
                    return om.readValue(hit.getSourceAsString(), type);
                } catch (Exception e) {
                    throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
                }
            }).collect(Collectors.toList());
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    public BulkByScrollResponse updateByQuery(QueryBuilder query, String index, String script)
            throws OpenSearchException {
        final String ctx = CLASSNAME + ".updateByQuery";
        try {
            UpdateByQueryRequest rq = new UpdateByQueryRequest();
            rq.indices(index);
            rq.setQuery(query);
            rq.setScript(new Script(script));
            rq.setRefresh(true);

            return client.updateByQuery(rq, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    public <T> IndexResponse index(String index, T document) throws OpenSearchException {
        final String ctx = CLASSNAME + ".index";
        try {
            IndexRequest rq = new IndexRequest();
            rq.index(index);
            rq.source(document);
            rq.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            return client.index(rq, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    public boolean indexExist(String index) throws OpenSearchException {
        final String ctx = CLASSNAME + ".indexExist";
        try {
            GetIndexRequest rq = new GetIndexRequest(index);
            GetIndexResponse rs = client.indices().get(rq, RequestOptions.DEFAULT);
            return !CollectionUtils.isEmpty(Arrays.asList(rs.getIndices()));
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    public void deleteIndex(List<String> indices) throws OpenSearchException {
        final String ctx = CLASSNAME + ".deleteIndex";
        try {
            DeleteIndexRequest rq = new DeleteIndexRequest(indices.toArray(new String[0]));
            client.indices().delete(rq, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new OpenSearchException(ctx + ": " + e.getLocalizedMessage());
        }
    }
}
