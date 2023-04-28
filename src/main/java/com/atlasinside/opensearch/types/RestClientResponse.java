package com.atlasinside.opensearch.types;

public class RestClientResponse {
    private final int responseCode;
    private final String body;

    public RestClientResponse(int responseCode, String body) {
        this.responseCode = responseCode;
        this.body = body;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getRawBody() {
        return body;
    }
}
