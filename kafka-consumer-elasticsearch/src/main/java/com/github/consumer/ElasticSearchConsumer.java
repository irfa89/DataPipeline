package com.github.consumer;

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


    public  static RestHighLevelClient createClient() {

         // https://XXXXXXXXXXXXX:XXXXXXXXXXXXX@XXXXXXXXXXXXX:443



        // Replace with credentials
           String hostname = "XXXXXXXXXXXXX";
           String username = "XXXXXXXXXXXXX";
           String password = "XXXXXXXXXXXXX";

            // bonsai  connection not required if Elastic Search installed locally
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            RestClientBuilder builder = RestClient.builder(
                    new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(
                    new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                            return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }

                    });
            RestHighLevelClient client = new RestHighLevelClient(builder);
            return client;
        }

    public static KafkaConsumer<String,String> createConsumer(String topic){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        // topic = "twitter_tweets";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); // disable auto commit of offsets.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");

        // create consumer configs
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe
        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }

    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJson){
        // gson library
        return jsonParser.parse(tweetJson).
                getAsJsonObject().
                get("id_str").
                getAsString();

    }


    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        // create Consumer
        KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");

        // poll for new data
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0

            Integer recordCount = records.count();
            logger.info("Received :" + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();


            // Insert data into Elastic Search ** Deafault is atleast once if not explicitly defined
            for (ConsumerRecord<String,String> record : records){

                 // Strategy 1 ** kafka genenric id
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // twitter feed specific id
                try {
                    // Consumer was made idempotent as parsing was based on id , which does not allow duplicates.
                    String id = extractIdFromTweet(record.value());

                    //String jsonString = record.value();

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id  // make consumers idempotent
                    ).source(record.value(), XContentType.JSON);

                    // add to bulk within no time.
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e){
                    logger.warn("Skipping bad data : " + record.value());
                }

                // Not needed as bulk request (   ** Performance Tunning.
                /*
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    String id = indexResponse.getId();
                    logger.info(indexResponse.getId()); )
                try {
                    Thread.sleep(10); // introduce a small delay
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } */
            }

            if (recordCount > 0) {
                // getting the response in bulk from client
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been commited..");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }



        // close client
        //client.close();

        }
    }
