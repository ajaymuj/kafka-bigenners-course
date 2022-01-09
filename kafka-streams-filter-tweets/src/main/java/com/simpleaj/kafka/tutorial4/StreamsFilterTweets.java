package com.simpleaj.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {

    public static final Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());
    private final static JsonParser jsonParser = new JsonParser();

    private static Integer extractUserFollowersInTweets(String tweetJson) {
        //gson library
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch (NullPointerException e) {
            logger.error("Failed parsing a tweet data!", e);
            return 0;
        }
    }

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder builder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = builder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) -> extractUserFollowersInTweets(jsonTweet) > 10000
                //filter for tweets from user which has followers over 10000
        );
        filteredStream.to("important_tweets");

        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);

        //start our streams application
        kafkaStreams.start();
    }

}
