package ae.pegasus.torascheduler;

import ae.pegasus.torascheduler.commons.ESAPILogger;
import ae.pegasus.torascheduler.commons.ESUtils;
import ae.pegasus.torascheduler.commons.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.*;


@Configuration
@EnableBatchProcessing
@EnableScheduling
public class BatchScheduler {

    private static final ESAPILogger logger =  new ESAPILogger(BatchScheduler.class);

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    SimpleJobLauncher jobLauncher;

    @Value("${twitter.keywords.split.count}")
    public int twitterKeywordsSplitCount;

    @Value("${instagram.keywords.split.count}")
    public int instagramKeywordsSplitCount;

    @Value("${twint.host}")
    private String twintHost;

    @Value("${instagram.host}")
    private String instagramHost;

    @Value("${tora.api.host}")
    private String toraApiHost;

    @Value("${twitter.stream.enabled:true}")
    private boolean isTwitterStreamEnabled;

    @Value("${instagram.stream.enabled:true}")
    private boolean isInstagramStreamEnabled;

    @Value("${twitter.stream.interactions.enabled:true}")
    private boolean isTwitterInteractionsEnabled;

    @Value("${twitter.stream.influencer.metrics.enabled:true}")
    private boolean isTwitterInfluencerMetrics;

    @Value("${twitter.users.split.count}")
    public int twitterUsersSplitCount;

    @Value("${twitter.influencer.host}")
    private String twitterInfluencerHost;

    @Value("${twitterinfluencermetrics.fixedRate.in.milliseconds}")
    private String twitterInfluencerMetricsRate;

    @Value("${tweets.fixedRate.in.milliseconds}")
    private String tweetsRate;

    @Value("${posts.fixedRate.in.milliseconds}")
    private String postsRate;

    @Value("${twitter.interactions.host}")
    private String twitterInteractionsHost;

    @Value("${retweetslikes.fixedRate.in.milliseconds}")
    private String retweetLikeRate;

    @Value("${instagram.stream.influencer.metrics.enabled:true}")
    private boolean isInstagramInfluencerMetrics;

    @Value("${instagram.users.split.count}")
    public int instagramUsersSplitCount;

    @Value("${instagram.influencer.host}")
    private String instagramInfluencerHost;

    @Value("${instagram.influencer.metrics.fixedRate.in.milliseconds}")
    private String instagramInfluencerMetricsRate;

    @Value("${tora_twitter_interactions_instances_count}")
    private int toraTwitterInteractionsInstancesCount;

    @Autowired
    private RestHighLevelClient elasticClient;

    @Autowired
    private ESUtils esUtils;

    @Value("${elastic.index.keywords}")
    private String keywordsIndex;

    public Job getTweetsJob() throws IOException {

        List<String> organizedKeywordsList = new ArrayList<String>();
        RestTemplate restTemplate = new RestTemplate();
        List<String> keywordsList=esUtils.getKeywords(keywordsIndex);
        String[] keywords = keywordsList.toArray(new String[keywordsList.size()]);
        int tweetLastXMinutes= (int) (Long.parseLong(tweetsRate)/(60*1000));
        tweetLastXMinutes=tweetLastXMinutes+1;
        Map<String, String> sinceUntil= Utils.sinceUntilUTC(tweetLastXMinutes);
        organizedKeywordsList = Utils.SplitAtOccurence(keywords,twitterKeywordsSplitCount);

        Flow masterFlow = new FlowBuilder<Flow>("masterFlow").start(mastertaskletStep("step1")).build();
        List<Flow> flowList = new ArrayList<>();
        for(String keyword : organizedKeywordsList)
        {
            Flow flowJob = new FlowBuilder<Flow>("flow1").start(tweetsTaskletStep(keyword, keyword, sinceUntil.get("since"), sinceUntil.get("until"))).build();
            flowList.add(flowJob);

        }

        Flow slaveFlow = new FlowBuilder<Flow>("splitflow")
                .split(new SimpleAsyncTaskExecutor()).add(flowList.stream().toArray(Flow[]::new)).build();

        return (jobBuilderFactory.get("getTweetsJob")
                .incrementer(new RunIdIncrementer())
                .start(masterFlow)
                .next(getTweetsDecider(keywords))
                .on("Cancel").to(cancelStep("No Keywords to Search For"))
                .from(getTweetsDecider(keywords)).on("Proceed").to(slaveFlow)
                .build()).build();

    }
    public Job getPostsJob() throws IOException {

        List<String> organizedKeywordsList = new ArrayList<String>();
        List<String> keywordsList=esUtils.getKeywords(keywordsIndex);
        String[] keywords = keywordsList.toArray(new String[keywordsList.size()]);
        int instaLastXMinutes= (int) (Long.parseLong(postsRate)/(60*1000));
        instaLastXMinutes=instaLastXMinutes+1;
        Map<String, String> sinceUntil= Utils.sinceUntilUTC(instaLastXMinutes);
        organizedKeywordsList = Utils.SplitAtOccurence(keywords,instagramKeywordsSplitCount);

        Flow masterFlow = new FlowBuilder<Flow>("masterFlow").start(mastertaskletStep("step1")).build();
        List<Flow> flowList = new ArrayList<>();
        for(String keyword : organizedKeywordsList)
        {
            Flow flowJob = new FlowBuilder<Flow>("flow1").start(postsTaskletStep(keyword, keyword, sinceUntil.get("since"),sinceUntil.get("until"))).build();
            flowList.add(flowJob);

        }

        Flow slaveFlow = new FlowBuilder<Flow>("splitflow")
                .split(new SimpleAsyncTaskExecutor()).add(flowList.stream().toArray(Flow[]::new)).build();

        return (jobBuilderFactory.get("getPostsJob")
                .incrementer(new RunIdIncrementer())
                .start(masterFlow)
                .next(getPostsDecider(keywords))
                .on("Cancel").to(cancelStep("No Keywords to Search For"))
                .from(getPostsDecider(keywords)).on("Proceed").to(slaveFlow)
                .build()).build();

    }


    public Step cancelStep(String message) {
        return stepBuilderFactory.get("cancelStep")
                .tasklet((contribution, chunkContext) -> {
                    logger.info(message);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    public Job getRetweetsAndLikesJob() throws IOException {

        List<List<Map<String,String>>> listsofretweetAndLikes = new ArrayList<List<Map<String,String>>>();
        List<Map<String,String>> retweetAndLikesCollection = new ArrayList<Map<String,String>>();
        ObjectMapper oMapper = new ObjectMapper();
        RestTemplate restTemplate = new RestTemplate();
        String url = toraApiHost + "/graph/get-tweet-ids-for-retweets";
        retweetAndLikesCollection = Arrays.asList(restTemplate.getForObject(url, Map[].class));
        if (retweetAndLikesCollection.size() > toraTwitterInteractionsInstancesCount) {
            listsofretweetAndLikes = Utils.NSizeParts(retweetAndLikesCollection, retweetAndLikesCollection.size() / toraTwitterInteractionsInstancesCount);
        }
        else{
            listsofretweetAndLikes = Utils.NSizeParts(retweetAndLikesCollection, toraTwitterInteractionsInstancesCount);

        }
        Flow masterFlow = new FlowBuilder<Flow>("masterFlow").start(mastertaskletStep("step1")).build();
        List<Flow> flowList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        for(List<Map<String,String>> lsmap : listsofretweetAndLikes)
        {
            try {
                String json = objectMapper.writeValueAsString(lsmap);
                Flow flowJob = new FlowBuilder<Flow>("flow1").start(RetweetsAndLikesTaskletStep(json, json)).build();
                flowList.add(flowJob);
            } catch (JsonProcessingException e) {
                logger.error("Failed in scheduling twitter interactions "+e);
            }

        }

        Flow slaveFlow = new FlowBuilder<Flow>("splitflow")
                .split(new SimpleAsyncTaskExecutor()).add(flowList.stream().toArray(Flow[]::new)).build();

        return (jobBuilderFactory.get("getRetweetsAndLikesJob")
                .incrementer(new RunIdIncrementer())
                .start(masterFlow)
                .next(getRetweetsAndLikessDecider(listsofretweetAndLikes))
                .on("Cancel").to(cancelStep("No Tweet Ids Returned"))
                .from(getRetweetsAndLikessDecider(listsofretweetAndLikes)).on("Proceed").to(slaveFlow)
                .build()).build();

    }


    public Job getTwitterInfluencerMetricsJob() throws IOException {


        List<List<Map<String,String>>> listofTwitterUsers = new ArrayList<List<Map<String,String>>>();
        List<Map<String,String>> twitterUsersCollection = new ArrayList<Map<String,String>>();
        ObjectMapper oMapper = new ObjectMapper();
        RestTemplate restTemplate = new RestTemplate();
        int twitterinfluencerLastXMinutes= (int) (Long.parseLong(twitterInfluencerMetricsRate)/(60*1000));
        twitterinfluencerLastXMinutes=twitterinfluencerLastXMinutes+1;
        logger.info("twitterinfluencerLastXMinutes: "+twitterinfluencerLastXMinutes);
        String url = toraApiHost + "/graph/get-twitter-users-for-influencer-metrics?lastMinutes="+twitterinfluencerLastXMinutes;
        Map<String, Integer> params = new HashMap<String, Integer>();

        params.put("lastMinutes", twitterinfluencerLastXMinutes);
        //twitterUsers=restTemplate.getForObject(url, Map[].class);
        twitterUsersCollection = Arrays.asList(restTemplate.getForObject(url, Map[].class));
        listofTwitterUsers = Utils.NSizeParts(twitterUsersCollection, twitterUsersSplitCount);
        Flow masterFlow = new FlowBuilder<Flow>("masterFlow").start(mastertaskletStep("step1")).build();
        List<Flow> flowList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        for(List<Map<String,String>> lsmap : listofTwitterUsers)
        {
            try {
                String json = objectMapper.writeValueAsString(lsmap);
                Flow flowJob = new FlowBuilder<Flow>("flow1").start(TwitterInfluencerMetricsTaskletStep(json, json)).build();
                flowList.add(flowJob);
            } catch (JsonProcessingException e) {
                logger.error("Failed in scheduling twitter influencer metrics "+e);
            }

        }

        Flow slaveFlow = new FlowBuilder<Flow>("splitflow")
                .split(new SimpleAsyncTaskExecutor()).add(flowList.stream().toArray(Flow[]::new)).build();

        return (jobBuilderFactory.get("getTwitterInfluencerMetricsJob")
                .incrementer(new RunIdIncrementer())
                .start(masterFlow)
                .next(getTwitterInfluencerDecider(listofTwitterUsers))
                .on("Cancel").to(cancelStep("No Twitter Users Returned"))
                .from(getTwitterInfluencerDecider(listofTwitterUsers)).on("Proceed").to(slaveFlow)
                .build()).build();

    }

    public Job getInstagramInfluencerMetricsJob() throws IOException {


        List<List<Map<String,String>>> listofInstagramUsers = new ArrayList<List<Map<String,String>>>();
        List<Map<String,String>> instagramUsersCollection = new ArrayList<Map<String,String>>();
        ObjectMapper oMapper = new ObjectMapper();
        RestTemplate restTemplate = new RestTemplate();
        int instagraminfluencerLastXMinutes= (int) (Long.parseLong(instagramInfluencerMetricsRate)/(60*1000));
        instagraminfluencerLastXMinutes=instagraminfluencerLastXMinutes+1;
        String url = toraApiHost + "/graph/get-instagram-users-for-influencer-metrics?lastMinutes="+instagraminfluencerLastXMinutes;
        Map<String, Integer> params = new HashMap<String, Integer>();

        params.put("lastMinutes", instagraminfluencerLastXMinutes);
        instagramUsersCollection = Arrays.asList(restTemplate.getForObject(url, Map[].class));
        listofInstagramUsers = Utils.NSizeParts(instagramUsersCollection, instagramUsersSplitCount);
        Flow masterFlow = new FlowBuilder<Flow>("masterFlow").start(mastertaskletStep("step1")).build();
        List<Flow> flowList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        for(List<Map<String,String>> lsmap : listofInstagramUsers)
        {
            try {
                String json = objectMapper.writeValueAsString(lsmap);
                Flow flowJob = new FlowBuilder<Flow>("flow1").start(instagramInfluencerMetricsTaskletStep(json, json)).build();
                flowList.add(flowJob);
            } catch (JsonProcessingException e) {
                logger.error("Failed in scheduling instagram influencer metrics"+e);
            }
        }

        Flow slaveFlow = new FlowBuilder<Flow>("splitflow")
                .split(new SimpleAsyncTaskExecutor()).add(flowList.stream().toArray(Flow[]::new)).build();

        return (jobBuilderFactory.get("getInstagramInfluencerMetricsJob")
                .incrementer(new RunIdIncrementer())
                .start(masterFlow)
                .next(getInstagramInfluencerDecider(listofInstagramUsers))
                .on("Cancel").to(cancelStep("No Instagram Users Returned"))
                .from(getInstagramInfluencerDecider(listofInstagramUsers)).on("Proceed").to(slaveFlow)
                .build()).build();

    }

    private TaskletStep tweetsTaskletStep(String keywords, String step, String since, String until) {
        return stepBuilderFactory.get(step).tasklet((contribution, chunkContext) -> {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            Map<String, Object> map = new HashMap<>();
            map.put("keywords", keywords);
            map.put("since", since);
            map.put("until", until);
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(map, headers);
            restTemplate.postForLocation(twintHost + "get_tweets_api", entity);
            return RepeatStatus.FINISHED;
        }).build();

    }


    private TaskletStep RetweetsAndLikesTaskletStep(String json, String step) {
        return stepBuilderFactory.get(step).tasklet((contribution, chunkContext) -> {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> request = new HttpEntity<String>(json, headers);
            restTemplate.postForLocation(twitterInteractionsHost + "retweets_likes", request);
            return RepeatStatus.FINISHED;
        }).build();

    }

    private TaskletStep TwitterInfluencerMetricsTaskletStep(String json, String step) {
        return stepBuilderFactory.get(step).tasklet((contribution, chunkContext) -> {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> request = new HttpEntity<String>(json, headers);
            restTemplate.postForLocation(twitterInfluencerHost + "get_twitter_influencer_metrics", request);
            return RepeatStatus.FINISHED;
        }).build();

    }


    private TaskletStep instagramInfluencerMetricsTaskletStep(String json, String step) {
        return stepBuilderFactory.get(step).tasklet((contribution, chunkContext) -> {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> request = new HttpEntity<String>(json, headers);
            restTemplate.postForLocation(instagramInfluencerHost + "get_instagram_influencer_metrics", request);
            return RepeatStatus.FINISHED;
        }).build();

    }



    private TaskletStep postsTaskletStep(String keywords, String step, String since, String until) {
        return stepBuilderFactory.get(step).tasklet((contribution, chunkContext) -> {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            Map<String, Object> map = new HashMap<>();
            map.put("keywords", keywords);
            map.put("since", since);
            map.put("until", until);
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(map, headers);
            restTemplate.postForLocation(instagramHost + "get_posts_api", entity);
            return RepeatStatus.FINISHED;
        }).build();

    }
    @Scheduled(initialDelayString = "${tweets.initialDelay.in.milliseconds}", fixedRateString = "${tweets.fixedRate.in.milliseconds}")
    public void getTweets() throws Exception {

        if(isTwitterStreamEnabled) {
            logger.info("Job Started at :" + new Date());

            JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(getTweetsJob(), param);

            logger.info("Job finished with status :" + execution.getStatus());
        }

    }

    @Scheduled(initialDelayString = "${retweetslikes.initialDelay.in.milliseconds}", fixedRateString = "${retweetslikes.fixedRate.in.milliseconds}")
    public void getTweetsAndLikes() throws Exception {

        if(isTwitterInteractionsEnabled) {
            logger.info("Job Started at :" + new Date());
            JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
                    .toJobParameters();
            JobExecution execution = jobLauncher.run(getRetweetsAndLikesJob(), param);
            logger.info("Job finished with status :" + execution.getStatus());
        }

    }


    @Scheduled(initialDelayString = "${twitterinfluencermetrics.initialDelay.in.milliseconds}", fixedRateString = "${twitterinfluencermetrics.fixedRate.in.milliseconds}")
    public void getTwitterInfluencerMetrics() throws Exception {

        if(isTwitterInfluencerMetrics) {
            logger.info("Job Started at :" + new Date());
            JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
                    .toJobParameters();
            JobExecution execution = jobLauncher.run(getTwitterInfluencerMetricsJob(), param);
            logger.info("Job finished with status :" + execution.getStatus());
        }

    }

    @Scheduled(initialDelayString = "${instagram.influencer.metrics.initialDelay.in.milliseconds}", fixedRateString = "${instagram.influencer.metrics.fixedRate.in.milliseconds}")
    public void getInstagramInfluencerMetrics() throws Exception {

        if(isInstagramInfluencerMetrics) {
            logger.info("Job Started at :" + new Date());
            JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
                    .toJobParameters();
            JobExecution execution = jobLauncher.run(getInstagramInfluencerMetricsJob(), param);
            logger.info("Job finished with status :" + execution.getStatus());
        }

    }

    @Scheduled(initialDelayString = "${posts.initialDelay.in.milliseconds}", fixedRateString = "${posts.fixedRate.in.milliseconds}")
    public void getPosts() throws Exception {

        if(isInstagramStreamEnabled) {
            logger.info("Instagram getPosts Job Started at :" + new Date());

            JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(getPostsJob(), param);

            logger.info("Job finished with status :" + execution.getStatus());
        }

    }
    public JobExecutionDecider getTweetsDecider(String[] keywords) {
        if(keywords.length > 0)
        {
            return (jobExecution, stepExecution) -> new FlowExecutionStatus("Proceed");
        }

        return (jobExecution, stepExecution) -> new FlowExecutionStatus("Cancel");
    }

    public JobExecutionDecider getRetweetsAndLikessDecider( List<List<Map<String,String>>> listofRetweetsAndLikesIds) {
        if(listofRetweetsAndLikesIds.isEmpty())
        {
            return (jobExecution, stepExecution) -> new FlowExecutionStatus("Cancel");
        }

        return (jobExecution, stepExecution) -> new FlowExecutionStatus("Proceed");
    }

    public JobExecutionDecider getTwitterInfluencerDecider( List<List<Map<String,String>>> listofTwitterUsers) {
        if(listofTwitterUsers.isEmpty())
        {
            return (jobExecution, stepExecution) -> new FlowExecutionStatus("Cancel");
        }

        return (jobExecution, stepExecution) -> new FlowExecutionStatus("Proceed");
    }

    public JobExecutionDecider getInstagramInfluencerDecider( List<List<Map<String,String>>> listofInstagramUsers) {
        if(listofInstagramUsers.isEmpty())
        {
            return (jobExecution, stepExecution) -> new FlowExecutionStatus("Cancel");
        }

        return (jobExecution, stepExecution) -> new FlowExecutionStatus("Proceed");
    }

    public JobExecutionDecider getPostsDecider(String[] keywords) {
        if(keywords.length > 0)
        {
            return (jobExecution, stepExecution) -> new FlowExecutionStatus("Proceed");
        }

        return (jobExecution, stepExecution) -> new FlowExecutionStatus("Cancel");
    }

    @Bean
    public ResourcelessTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }


    private TaskletStep mastertaskletStep(String step) {
        return stepBuilderFactory.get(step).tasklet((contribution, chunkContext) -> {
          logger.info("Parallel Execution of Api Calls to Tora Twint");
            return RepeatStatus.FINISHED;
        }).build();

    }

    @Bean
    public MapJobRepositoryFactoryBean mapJobRepositoryFactory(ResourcelessTransactionManager txManager)
            throws Exception {

        MapJobRepositoryFactoryBean factory = new MapJobRepositoryFactoryBean(txManager);

        factory.afterPropertiesSet();

        return factory;
    }

    @Bean
    public JobRepository jobRepository(MapJobRepositoryFactoryBean factory) throws Exception {
        return factory.getObject();
    }

    @Bean
    public SimpleJobLauncher jobLauncher(JobRepository jobRepository) {
        SimpleJobLauncher launcher = new SimpleJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return launcher;
    }




}
