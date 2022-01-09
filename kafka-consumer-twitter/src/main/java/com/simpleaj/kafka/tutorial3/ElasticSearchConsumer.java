package com.simpleaj.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    public static RestHighLevelClient createClient() {

//        Full access URL - https://np33b4if1z:vn0ci3ipib@kafka-course-aj-4633487484.us-east-1.bonsaisearch.net:443
//        Access Key - np33b4if1z
//        Access secret - vn0ci3ipib
        String hostname = "kafka-course-aj-4633487484.us-east-1.bonsaisearch.net";
        String username = "np33b4if1z";
        String password = "vn0ci3ipib";

        //dont do if you run a local elastic search
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //earliest/latest/none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        //create consumer
        KafkaConsumer<String, String> consumer =  new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) {
        String topic = "twitter_tweets";
        RestHighLevelClient client = createClient();
//        String jsonStr = "{ \"foo\" : \"bar\" }";
        try {
            KafkaConsumer<String, String> consumer = createConsumer(topic);
            //poll for new data
            while(true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                logger.info("Received : {} records!", records.count());
                //performance improvement using batching
                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record : records) {
                    // this is where we are going to insert data into elastic search
//                    String jsonStr = record.value();

                    //2 strategies to create id's to make consumer idempotent
                    //1. kafka generic ID
//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    //2. twitter feed specific ID
                    String id = extractIdFromTweet(record.value());

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id //this is to make our consumer idempotent
                    ).source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest); //add to bulk request (takes no time)


//                    committed below code for batch processing
//                    IndexResponse reponse = client.index(indexRequest, RequestOptions.DEFAULT);
//                    String resId = reponse.getId();
//                    logger.info("response id : " + resId);
//
//                    try {
//                        Thread.sleep(10);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                }

                if(records.count() > 0) {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("committing offsets...");
                    consumer.commitSync();
                    logger.info("offsets have been committed...");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            //close the client gracefully
//            client.close();
        } catch (IOException e) {
            logger.error("Failed to set index ", e);
        }

    }

    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJson) {
        //gson library
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
}
