package com.github.tinni2806.kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

  private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

  private List<String> terms = Lists.newArrayList("covid-19", "coronavirus", "pandemic");

  private TwitterProducer() {}

  public static void main(String[] args) throws InterruptedException {
    new TwitterProducer().run();
  }

  private void run() throws InterruptedException {
    File file = new File("src/main/java/com/github/tinni2806/kafka/config.java");
    boolean exists = file.exists();
    if (exists) {
      logger.info("config.java exists");
    } else {
      logger.error("config.java file not found!");
      logger.error("Exiting: Please create a config.java file to continue");
      System.exit(0);
    }

    logger.info("Setup");
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
    Client client = createTwitterClient(msgQueue);
    client.connect();
    KafkaProducer<String, String> producer = createKafkaProducer();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Shutdown Hook:");
                  logger.info("Shutting down Twitter Client");
                  client.stop();
                  logger.info("Closing Producer");
                  producer.close();
                  logger.info("Shutdown completed");
                }));
    while (!client.isDone()) {
      String msg = null;
      msg = msgQueue.poll(5, TimeUnit.SECONDS);
      if (msg != null) {
        logger.info(msg);
        producer.send(
            new ProducerRecord<>("TwitterTopic", null, msg),
            (recordMetadata, e) -> {
              if (e != null) {
                logger.error("Exception: ", e);
              }
            });
      }
    }
    logger.info("End of application");
  }

  private Client createTwitterClient(BlockingQueue<String> msgQueue) {
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    hosebirdEndpoint.trackTerms(terms);
    Authentication hosebirdAuth =
        new OAuth1(config.consumerKey, config.consumerSecret, config.token, config.tokenSecret);
    ClientBuilder builder =
        new ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));
    return builder.build();
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    String bootStrapServers = "localhost:9092";
    String topic = "TwitterTopic";
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
    return new KafkaProducer<String, String>(properties);
  }
}
