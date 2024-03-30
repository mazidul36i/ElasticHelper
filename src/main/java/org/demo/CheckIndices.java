package org.demo;

import org.demo.utility.ElasticClient;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CheckIndices {

    private final static RestHighLevelClient highLevelClient = new ElasticClient().getClient();

    public static void main(String[] args) {

        String objectType = "1008";
        String serverName = "dowqa.masterdataonline.com";
        String schemaId = "4805460525145";


        // get correction and do_br indices of the same module
        GetIndexRequest getIndexRequest = new GetIndexRequest(serverName + "_" + objectType + "_nos_corr_*", serverName + "_" + objectType + "_do_br_*_");
        getIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
//        getIndexRequest.indicesOptions(new IndicesOptions(EnumSet.of(IndicesOptions.Option.ALLOW_NO_INDICES), EnumSet.of(IndicesOptions.WildcardStates.OPEN)));

        List<String> docIds = Arrays.asList("G111-HIST__-HIST__-FL_TEST610_CNCL", "G111-HIST__-HIST__-FL_TEST579_CNCL", "G111-HIST__-HIST__-FL_TEST760_CNCL");

        try {
            GetIndexResponse response = highLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);

            System.out.println(response.getSettings());
            System.out.println(response.getMappings());

            List<String> indices = Stream.of(response.getIndices()).filter(index -> !index.contains(schemaId)).collect(Collectors.toList());
            indices.add("Test_index");
//            System.out.println("List of indices on the module to be update: " + Arrays.toString(indices));

            UpdateByQueryRequest updateByQuery = new UpdateByQueryRequest();
            updateByQuery.indices(indices.toArray(String[]::new));
            BoolQueryBuilder boolQuery =  new BoolQueryBuilder();
            boolQuery.must(QueryBuilders.termsQuery("id.keyword", docIds));
            updateByQuery.setQuery(boolQuery);
            Script script =  new Script("ctx._source._outdated=true");
            updateByQuery.setScript(script);
            updateByQuery.setRefresh(true);

            System.out.println(updateByQuery.indicesOptions());
            updateByQuery.setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false));
            System.out.println(updateByQuery.indicesOptions());

            try {
                highLevelClient.updateByQuery(updateByQuery, RequestOptions.DEFAULT);
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            highLevelClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
