package com.atlasinside.opensearch.clients;

import com.atlasinside.opensearch.types.RestClientResponse;
import com.atlasinside.opensearch.util.Constants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import okhttp3.*;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
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
                .addInterceptor(new ErrorHandlerInterceptor())
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    /**
     * Execute a GET request
     *
     * @param uri         Uri of the request
     * @param queryParams A map with the query parameters
     */
    public RestClientResponse get(String uri, Map<String, String> queryParams) {
        try {
            HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(BASEURL + uri)).newBuilder();

            if (!MapUtils.isEmpty(queryParams))
                queryParams.forEach(urlBuilder::addEncodedQueryParameter);
            Request request = new Request.Builder().url(urlBuilder.build()).build();
            RestClientResponse response;
            try (Response rs = client.newCall(request).execute()) {
                response = new RestClientResponse(rs.code(), Objects.requireNonNull(rs.body(), "Response body is null").string());
            }
            return response;
        } catch (Exception e) {
            throw new RuntimeException(e.getLocalizedMessage());
        }
    }

    private static class ErrorHandlerInterceptor implements Interceptor {
        @NotNull
        @Override
        public Response intercept(@NotNull Chain chain) throws IOException {
            Response response = chain.proceed(chain.request());

            if (!response.isSuccessful()) {
                Gson gson = new Gson();
                String body = gson.toJson(new RestClientResponse(response.code(), "The response from the server was not OK"));
                ResponseBody responseBody = ResponseBody.create(body, MediaType.parse(Constants.APPLICATION_JSON_VALUE));

                ResponseBody originalBody = response.body();
                if (originalBody != null) {
                    originalBody.close();
                }

                return response.newBuilder().body(responseBody).build();
            }
            return response;
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
