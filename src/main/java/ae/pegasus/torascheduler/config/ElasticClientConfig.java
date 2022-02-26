package ae.pegasus.torascheduler.config;

import ae.pegasus.torascheduler.commons.ESAPILogger;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


    @Configuration
    public class ElasticClientConfig {

        private static final ESAPILogger logger = new ESAPILogger(ElasticClientConfig.class);

        @Value("${elastic.host}")
        private String elasticHost;

        @Value("${elastic.port}")
        private String elasticPort;

        @Value("${elastic.scheme}")
        private String elasticScheme;

        @Value("${elastic.username}")
        private String elasticUsername;

        @Value("${elastic.password}")
        private String elasticPassword;

        @Bean
        public RestHighLevelClient elasticClient() {
            RestHighLevelClient client = null;
            if (elasticScheme.equalsIgnoreCase("http")) {
                client= new RestHighLevelClient(RestClient.builder(new HttpHost(elasticHost, Integer.valueOf(elasticPort), elasticScheme)));
            } else if (elasticScheme.equalsIgnoreCase("https")) {
                logger.info("creating secure elastic search connection");
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticUsername, elasticPassword));
                RestClientBuilder builder = RestClient.builder(new HttpHost(elasticHost, Integer.valueOf(elasticPort), elasticScheme))
                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                            }
                        });

                client = new RestHighLevelClient(builder);
            }
            return client;
        }


    }


