package com.utmstack.opensearch_connector.clients;

import org.apache.commons.lang3.StringUtils;
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
import org.opensearch.client.transport.rest_client.RestClientTransport;

import javax.net.ssl.SSLContext;
import java.util.Objects;

public class OpensearchClient {
    private static final String CLASSNAME = "OpensearchClient";

    public static OpenSearchClient build(String user, String password, HttpHost host) {
        final String ctx = CLASSNAME + ".build";
        try {
            if (Objects.isNull(host))
                throw new RuntimeException("No hosts definition were provided");

            SSLContextBuilder sslBuilder = SSLContexts.custom()
                    .loadTrustMaterial(null, (x509Certificates, s) -> true);
            final SSLContext sslContext = sslBuilder.build();

            RestClient restClient;
            if (!StringUtils.isEmpty(user) && !StringUtils.isEmpty(password)) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
                restClient = RestClient.builder(host)
                        .setHttpClientConfigCallback(builder -> builder
                                .setSSLContext(sslContext)
                                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                .setDefaultCredentialsProvider(credentialsProvider))
                        .build();
            } else {
                restClient = RestClient.builder(host)
                        .setHttpClientConfigCallback(builder -> builder
                                .setSSLContext(sslContext)
                                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE))
                        .build();
            }
            return new OpenSearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }
}
