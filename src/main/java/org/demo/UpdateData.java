package org.demo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.lang.reflect.Executable;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class UpdateData {

    public static String ELASTIC_HOST = "localhost";
    public static int ELASTIC_PORT = 9200;
    public static String HTTP_SCHEME = "http";
    public static String ELASTIC_INDEX = "dowqa.masterdataonline.com_1008_nos_corr_4805460525145";

    static RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(ELASTIC_HOST, ELASTIC_PORT, HTTP_SCHEME)));

    static int batchSize = 10;

    public static void main(String[] args) {

        try {
            System.out.println("======= START =====\n\n");

            int elasticBatchSize = 1000;
            SearchSourceBuilder searchSourceObjNrs = new SearchSourceBuilder();
            SearchRequest searchRequestObjNrs = new SearchRequest();
            searchSourceObjNrs.fetchSource(false);
            searchRequestObjNrs.indices(ELASTIC_INDEX);
            searchSourceObjNrs.size(elasticBatchSize);
//            searchSourceObjNrs.query(boolQuery);
            searchRequestObjNrs.source(searchSourceObjNrs);
            searchRequestObjNrs.scroll(TimeValue.timeValueMinutes(2L));

            SearchResponse searchResponse = client.search(searchRequestObjNrs, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            int batchNo = 1;
            for (int i = 0; i < 1000; i += elasticBatchSize, batchNo++) {
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
                searchScrollRequest.scrollId(scrollId);
                searchScrollRequest.scroll(TimeValue.timeValueMinutes(2L));
                try {
                    searchResponse = i == 0 ? searchResponse
                            : client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    try {
                        searchResponse = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                    } catch (Exception ex1) {
                        System.out.println("error getting respoonse again");
                        ex1.printStackTrace();
                    }
                }
                scrollId = searchResponse.getScrollId();

                List<String> objectNumbers = Arrays.stream(searchResponse.getHits().getHits())
                        .map(SearchHit::getId).distinct().collect(Collectors.toList());

                System.out.println("scrollId " + scrollId + ", objectNumbers " + objectNumbers.size() + " and batchNo " + batchNo);

                runInBatch(objectNumbers);
            }

            System.out.println("\n\nEnd with success");

        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            System.out.println("Closing client");
            client.close();
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

    public static void runInBatch(List<String> docIds) {
        for (int i = 0; i < docIds.size(); i += batchSize) {

            System.out.println("Batch number: " + i);

            List<String> objectNumbers = docIds.subList(i, Math.min(i + batchSize, docIds.size()));

            String userName = UUID.randomUUID().toString();
            updateCorrApprovalOnPushToPrimeSave(4805460525145L, userName, ELASTIC_INDEX, objectNumbers);
        }
    }

    private static void updateCorrApprovalOnPushToPrimeSave(Long schemaId, String userName, String
            correctionIndex, List<String> docIds) {
        // Update docs, mark as isReviewed true... after approved, and sent to queue for primeSave.
//        log.info("updateCorrApprovalOnPushToPrimeSave - Start schemaId {}, userName {}, docIds {}", schemaId, userName, docIds);

        System.out.println("\n\n\n===== Start to update =====");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termsQuery("id.keyword", docIds));

        searchSourceBuilder.size(docIds.size());
        searchSourceBuilder.query(boolQueryBuilder);

        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(correctionIndex);

        StringBuffer scr = new StringBuffer();
        scr.append("ctx._source.isReviewed = ").append(true).append("; ");
        scr.append("ctx._source.reviewedAt = params.reviewedAt; ");
        scr.append("ctx._source.reviewedBy = params.reviewedBy; ");
        scr.append("ctx._source.isSubmitted = params.isSubmitted; ");
        scr.append("ctx._source.submittedAt = params.reviewedAt; ");
        scr.append("ctx._source.submittedBy = params.reviewedBy; ");
        scr.append("ctx._source.correction_status = params.correction_status");

        final Map<String, Object> params = new HashMap<>();
        params.put("isReviewed", Boolean.TRUE);
        params.put("isSubmitted", Boolean.TRUE);
        params.put("reviewedBy", userName);
        params.put("reviewedAt", Instant.now().toEpochMilli());
        params.put("correction_status", "Sent");

        final Script script = new Script(ScriptType.INLINE, "painless", scr.toString(), params);
        System.out.println("update script for corr approval " + script);
        updateByQueryRequest.setRefresh(Boolean.TRUE);
        updateByQueryRequest.setTimeout(TimeValue.MINUS_ONE);
        updateByQueryRequest.setScript(script);
        updateByQueryRequest.getSearchRequest().source(searchSourceBuilder);

        try {
            client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("updateCorrApprovalOnPushToPrimeSave - End schemaId " + schemaId);
    }

}
