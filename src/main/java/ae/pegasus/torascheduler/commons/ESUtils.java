package ae.pegasus.torascheduler.commons;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Configuration
public class ESUtils {

    private static final ESAPILogger logger =  new ESAPILogger(ESUtils.class);


    private  RestHighLevelClient elasticClient;

    public ESUtils(RestHighLevelClient elasticClient){
     this.elasticClient=elasticClient;
 }


    public  List<String> getKeywords(String indexName) throws IOException {
        SearchRequest request = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // high value of size to make sure to get all keywords
        searchSourceBuilder.size(2000);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);

        SearchResponse response = elasticClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        List<String> keywords = new ArrayList<>();
        for (SearchHit hit : hits.getHits()) {
            Map<String, Object> sourceAsMap = (Map<String, Object>) hit.getSourceAsMap();
            keywords.add((String) sourceAsMap.get("title"));
        }

        logger.info(String.format("getKeywords(): %s", keywords));
        return keywords;
    }
}
