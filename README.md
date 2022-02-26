# tora-scheduler

Scheduler Application for running schedule

- The Tora-Scheduler(Java Spring Boot Application Created for Running Schedulers) Executes scheduler after every configured Nth Interval and calls get_tweets method of tora-twint with parameters namely
  keywords, since and until. The tora-twint Publish the tweets to RABBITMQ Queue named Tweets
- The Tora-Scheduler(Java Application Created for Running Schedulers) Executes scheduler after every configured Nth Interval to Get user profiles who Retweeted and Liked the tweet which matches the tweet
  Ids which were fetched from the ToraApi and calls get_retweets_likes action method of tora-twint to Get User Profiles Who liked or Retweeted. The user profiles are published to RABBITMQ Queue named interactions
- The Tora-Scheduler(Java Spring Boot Application Created for Running Schedulers) Executes scheduler after every configured Nth Interval and calls get_posts_api method of tora-instagram with parameters namely
  keywords, since and until. The tora-instagram publishes the instagram posts to RABBITMQ Queue named instagram

## Build Instructions

To build the image:

```
source sh build.sh
```

To deploy it on Mac:

```
docker run -it -d -p 9010:9010 -e KEYWORDS_SPLIT_COUNT=5 -e TWINT_HOST='http://host.docker.internal:5000' -e TORA_API_HOST="http://host.docker.internal:8080" -e TWITTER_STREAM_ENABLED=true -e TWITTER_STREAM_INTERACTIONS_ENABLED=true -e INSTAGRAM_STREAM_ENABLED=true -e INSTAGRAM_HOST=<INSTAGRAM_HOST> --rm --name tora-scheduler tora-scheduler:latest
```
