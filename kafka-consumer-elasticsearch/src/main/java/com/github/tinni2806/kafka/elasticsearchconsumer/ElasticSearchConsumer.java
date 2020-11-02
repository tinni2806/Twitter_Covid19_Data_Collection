package com.github.tinni2806.kafka.elasticsearchconsumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

  private static RestHighLevelClient createClient() {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials(config.username, config.password));

    RestClientBuilder builder =
        RestClient.builder(new HttpHost(config.hostname, 443, "https"))
            .setHttpClientConfigCallback(
                httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

    return new RestHighLevelClient(builder);
  }

  private static KafkaConsumer<String, String> createConsumer(String topic) {
    String bootstrapServers = "localhost:9092";
    String groupId = "Twitter-Consumer-Group";
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topic));
    return consumer;
  }

  private static JsonParser jsonParser = new JsonParser();

  private static String extractIdFromTweet(String tweetJson) {
    return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    RestHighLevelClient client = createClient();
    KafkaConsumer<String, String> consumer = createConsumer("TwitterTopic");
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Shutdown Hook Called:");
                  logger.info("Shutting down Elasticsearch Client");
                  try {
                    client.close();
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                  logger.info("Shutdown completed");
                }));
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      int recordCount = records.count();
      logger.info("Received " + recordCount + " records");
      BulkRequest bulkRequest = new BulkRequest();
      for (ConsumerRecord<String, String> record : records) {
        try {
          String id = extractIdFromTweet(record.value());
          IndexRequest indexRequest =
              new IndexRequest("twitter").source(record.value(), XContentType.JSON).id(id);
          bulkRequest.add(indexRequest);
        } catch (NullPointerException e) {
          logger.warn("skipping bad data: Missing id_str" + record.value());
        }
      }
      if (recordCount > 0) {
        BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        logger.info("Committing offsets...");
        consumer.commitSync();
        logger.info("Offsets have been committed");
        Thread.sleep(1000);
      }
    }
  }
}
