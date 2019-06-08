package com.github.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    private static JsonParser jsonParser = new JsonParser();
    private static Integer extractUserFollowersInTweets(String tweetJson){
        // gson library
        try {
            return jsonParser.parse(tweetJson).
                        getAsJsonObject().
                        get("user").
                        getAsJsonObject().
                        get("followers_count").
                        getAsInt();

        }catch (NullPointerException e){
            return 0;
        }
    }

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String applicationId = "demo-kafka-streams";
        String topic = "twitter_tweets";
        String outputTopic = "important_tweets";

        // create properties
        Properties properties  = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,applicationId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String,String> inputTopic = streamsBuilder.stream(topic);
        KStream<String,String> filteredStream = inputTopic.filter(
                // filter for tweets which has a user of 1000 followers
                (k,jsonTwees) -> extractUserFollowersInTweets(jsonTwees) > 1000
        );
        filteredStream.to(outputTopic);

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);

        // start stream application
        kafkaStreams.start();

    }
}
