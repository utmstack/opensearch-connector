package com.atlasinside.opensearch.clients;

import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.apache.http.HttpHost;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

public class RestClient {
    private static final String CLASSNAME = "RestClient";

    private final WebClient client;

    public RestClient(String user, String password, HttpHost host) {
        final String ctx = CLASSNAME + ".build";
        try {
            final HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
            headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
            headers.setBasicAuth(user, password);

            final int size = 16 * 1024 * 1024;
            final ExchangeStrategies strategies = ExchangeStrategies.builder()
                    .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(size))
                    .build();

            SslContext sslContext = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();

            HttpClient httpClient = HttpClient.create()
                    .secure(s -> s.sslContext(sslContext))
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)
                    .doOnConnected(connection -> {
                        connection.addHandlerLast(new ReadTimeoutHandler(30));
                        connection.addHandlerLast(new WriteTimeoutHandler(30));
                    });

            client = WebClient.builder().baseUrl(host.toString())
                    .defaultHeaders(h -> h.addAll(headers))
                    .clientConnector(new ReactorClientHttpConnector(httpClient))
                    .exchangeStrategies(strategies)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    /**
     * Execute a GET request
     *
     * @param uri         Uri of the request
     * @param type        Type to map the response
     * @param queryParams A map with the query parameters
     * @param <T>         Generic representation of the response
     * @return
     */
    public <T> T get(String uri, MultiValueMap<String, String> queryParams, Class<T> type) {
        final String ctx = CLASSNAME + ".get";
        return client.get().uri(uriBuilder -> uriBuilder.path(uri).queryParams(queryParams).build())
                .retrieve().onStatus(HttpStatus::isError, response -> response.bodyToMono(String.class)
                        .handle((error, sink) -> sink.error(new RuntimeException(ctx + ": " + error))))
                .bodyToMono(type).share().block();
    }
}
