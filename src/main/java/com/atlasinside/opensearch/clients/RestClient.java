package com.atlasinside.opensearch.clients;

import com.atlasinside.opensearch.util.Constants;
import com.google.gson.Gson;
import okhttp3.*;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class RestClient {
    private static final String CLASSNAME = "RestClient";
    private final OkHttpClient client;
    private static String BASEURL;
    private static String USER;
    private static String PASS;

    public RestClient(String user, String password, HttpHost host) {
        BASEURL = host.toString();
        USER = user;
        PASS = password;
        client = new OkHttpClient.Builder()
                .addInterceptor(new RequestHandlerInterceptor())
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    /**
     * Execute a GET request
     *
     * @param uri         Uri of the request
     * @param queryParams A map with the query parameters
     */
    public Response get(String uri, Map<String, String> queryParams) {
        final String ctx = CLASSNAME + ".get";
        try {
            if (StringUtils.isNotBlank(uri) && !uri.startsWith("/"))
                uri = "/" + uri;
            HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(BASEURL + uri))
                    .newBuilder();
            if (!MapUtils.isEmpty(queryParams))
                queryParams.forEach(urlBuilder::addEncodedQueryParameter);
            Request request = new Request.Builder().url(urlBuilder.build()).build();
            return client.newCall(request).execute();
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    public Response put(String uri, Map<String, String> queryParams, Object body) {
        final String ctx = CLASSNAME + ".put";
        try {
            if (StringUtils.isNotBlank(uri) && !uri.startsWith("/"))
                uri = "/" + uri;
            HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(BASEURL + uri))
                    .newBuilder();
            if (!MapUtils.isEmpty(queryParams))
                queryParams.forEach(urlBuilder::addEncodedQueryParameter);
            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            Request request = new Request.Builder()
                    .url(urlBuilder.build())
                    .put(RequestBody.create(new Gson().toJson(body), JSON))
                    .build();
            return client.newCall(request).execute();
        } catch (Exception e) {
            throw new RuntimeException(ctx + ": " + e.getLocalizedMessage());
        }
    }

    private static class RequestHandlerInterceptor implements Interceptor {
        @NotNull
        @Override
        public Response intercept(@NotNull Chain chain) throws IOException {
            Request originalRequest = chain.request();
            Request.Builder requestBuilder = originalRequest.newBuilder()
                    .header(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON_VALUE)
                    .header(Constants.ACCEPT, Constants.APPLICATION_JSON_VALUE);

            if (!StringUtils.isEmpty(USER) && !StringUtils.isEmpty(PASS))
                requestBuilder.header(Constants.AUTHORIZATION, Credentials.basic(USER, PASS, Charset.defaultCharset()));

            return chain.proceed(requestBuilder.build());
        }
    }
}
