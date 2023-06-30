package org.demo;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static String ELASTIC_HOST = "localhost";
    public static int ELASTIC_PORT = 9200;
    public static String HTTP_SCHEME = "http";
    public static String ELASTIC_INDEX = "localhost_1007_do_0_en";

    static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost(ELASTIC_HOST, ELASTIC_PORT, HTTP_SCHEME))
    );

    public static void main(String[] args) {
        System.out.println("Started Application!");
        System.out.println("Reading file");

        String filePath = "elastic_data.json";
        String jsonString = readJsonFromFile(filePath);

        Gson gson = new Gson();
        JsonArray jsonArray = gson.fromJson(jsonString, JsonArray.class);
        System.out.println("JSON Array size: " + jsonArray.size());

        System.out.println("<================ Hitting http request =============>");

        for (JsonElement jsonElement : jsonArray) {
            Map<String, Object> data = gson.fromJson(jsonElement, Map.class);

            String id = String.valueOf(data.get("_id"));
            String actualData = gson.toJson(data.get("_source"));

            insertUpdateData(actualData, id);
        }

        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void insertUpdateData(String jsonData, String id) {

        IndexRequest request = new IndexRequest(ELASTIC_INDEX)
                .id(id)
                .source(jsonData, XContentType.JSON);

        try {
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("\\u001B[31mError insert data with id: " + id + "\\u001B[0m");
            e.printStackTrace();
        }
    }

    public static String readJsonFromFile(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            StringBuilder jsonBuilder = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                jsonBuilder.append(line);
            }

            String json = jsonBuilder.toString();
            System.out.println("Reading file successfully");
            return json;
        } catch (IOException e) {
            throw new RuntimeException("Error reading JSON file: " + e.getMessage());
        }
    }
}

class JsonQuoteFixer {

    public static String filePath = "sample.txt";

    public static void main(String[] args) {
        String json = Main.readJsonFromFile(filePath);
        Gson gson = new Gson();

        String pattern = "\": \"(.*?)\",";

        Pattern regex = Pattern.compile(pattern);
        Matcher matcher = regex.matcher(json);

        while (matcher.find()) {
            String extractedValue = matcher.group(1);
            System.out.println("Fixing data: " + extractedValue);
            json = json.replace(extractedValue, extractedValue.replaceAll("\"", "'"));
        }

        JsonObject object = gson.fromJson(json, JsonObject.class);

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(String.valueOf(object));
            System.out.println("Data has been written to the file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the file: " + e.getMessage());
        }
    }
}