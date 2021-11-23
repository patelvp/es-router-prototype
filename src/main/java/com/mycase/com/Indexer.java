package com.mycase.com;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Indexer {
  public static void main(String[] args) {
    RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9201, "http"));
    // And create the API client
    RestHighLevelClient client = new RestHighLevelClient(builder);

    // properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-third-application12");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    consumer.subscribe(Collections.singleton("es-index-server-1"));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(record);
        IndexRequest request = new IndexRequest("generic-index-pool", "_doc").source(record.value(), XContentType.JSON);
        request.id(record.key());
        try {
          IndexResponse response = client.index(request, RequestOptions.DEFAULT);
          System.out.println(response);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
