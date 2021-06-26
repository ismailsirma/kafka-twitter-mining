package com.sirmam.kafka.tutorial3;

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
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    private static JsonParser jsonParser = new JsonParser();

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // when producer takes a string, it serializes it into bytes and send it to kafka
        // when kafka send these bytes right back to consumer, consumer has to take bytes and create string from them
        // from byte to string process is deserialization
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // THREE OPTIONS : earliest/latest/none
        // earliest : read from beginning of the topic
        // latest : only the new messages, onwards
        // none : will throw an error, if no assets being saved
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        // amount of maximum number of data record received at each poll
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");

        // create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        //consumer.subscribe(Arrays.asList("first_topic", "second_topic"));

        return consumer;
    }

    public static RestHighLevelClient createClient(){

        String hostname = "localhost";
        String username = "";
        String password = "";

        // do not need to do this if you run on local machine (it is for cloud)
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        // encrypted connection to the cloud
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))

                // apply credentials to https call
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();

        // create the kafka consumer
        KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");

        // poll for new data for 100ms
        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0

            logger.info("Received " + records.count() + " records");
            for(ConsumerRecord<String, String> record : records){

                // 2 strategies to make ID unique
                // 1 : Kafka Generic ID
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                // 2 : Specific ID for the data stream, Twitter Feed SpecificId
                String id = extranctIdFromTweet(record.value());

                logger.info("Key: " + record.key() + ", Value : " + record.value() );
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset() );

                // insert data into ElasticSearch ----- START

                // create an elastic search index request obj
                // make sure that index exists on ES
                IndexRequest indexRequest = new IndexRequest(
                        "twitter", "tweet", id // to make our consumer idempotent
                ).source(
                        record.value(), XContentType.JSON
                );

                // Run the request on ES
                // data that we passed is inserted into elastic search
                IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                // insert data into ElasticSearch ----- END

                // retrieve elasticsearch document id and log it
                logger.info(response.getId());

                // introduce a small delay before committing
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
            logger.info("Committing offsets...");
            consumer.commitAsync();
            logger.info("Offsets have been committed");
            // to see output slower on the console
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e){
                e.printStackTrace();
            }

            // finally close the client
            client.close();
        }
        // end of while loop

    }

    public static String extranctIdFromTweet(String tweetJson){

        //gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
