package org.demo.utility;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class ElasticClient {

    private static final String ELASTIC_HOST = "localhost";
    private static final int ELASTIC_PORT = 9200;
    private static final String HTTP_SCHEME = "http";

    private static final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost(ELASTIC_HOST, ELASTIC_PORT, HTTP_SCHEME))
    );

    public RestHighLevelClient getClient() {
        return client;
    }

    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
