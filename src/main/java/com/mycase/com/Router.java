
package com.mycase.com;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Router {
  static Logger logger;

  public static void main(String[] args) {
    logger = LoggerFactory.getLogger(Router.class.getName());
    System.out.println("Hello World!");
//    producer();
    consumer();
  }

  private static void producer(String key, String value) {
    // create properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    // send data
    ProducerRecord<String, String> record = new ProducerRecord<>("es-index-server-1", key, value);
    producer.send(record, new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null){
          System.out.println("Forwarding");
          logger.info("Topic:" + metadata.topic() +
              "\nPartition:" + metadata.partition() +
              "\nOffset:" + metadata.offset() +
              "\nTimestamp:" + metadata.timestamp());
        }
        else{
          exception.printStackTrace();
        }
      }
    });
    producer.flush();
  }

  private static void consumer() {
    // properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-third-application");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    consumer.subscribe(Collections.singleton("es_indexing"));

    while(true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for(ConsumerRecord<String, String> record : records) {
        // Based on if the firm opts in for document search. We will need to also send this exact message to another producer
        producer(record.key(), record.value());
      }
    }
  }
}
