package org.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.demo.utility.ElasticClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Map;

public class CreateIndex {

    private static RestHighLevelClient highLevelClient = new ElasticClient().getClient();;

    public static void main(String[] args) throws JsonProcessingException {

        String sourcePath = "index_create_request.json";


        // localhost_cr_do_346377_133069_en
        String sourceJson = Main.readJsonFromFile(sourcePath);
        CreateIndexRequest request = new CreateIndexRequest("localhost_133069_do_346377_en");

        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(sourceJson, JsonObject.class);
        jsonObject.getAsJsonObject("mappings");


        try {
            highLevelClient.indices().create(request, RequestOptions.DEFAULT);
            highLevelClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


}
