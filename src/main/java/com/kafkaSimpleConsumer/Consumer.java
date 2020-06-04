package com.kafkaSimpleConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(Consumer.class);

		String bootstrapServer = "localhost:9092";
		String groupId = "my-kafka-consumer";
		String topic = "fst_topic";

		// Create producer properties
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// Create consumer client
		@SuppressWarnings("resource") // This is suppressed as its demo program
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		// Subscribe consumer to topics
		consumer.subscribe(Collections.singleton(topic));

		// poll for new data
		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				logger.info("Topic: " + consumerRecord.topic());
				logger.info("Partition: " + consumerRecord.partition());
				logger.info("Offset: " + consumerRecord.offset());
				logger.info("Key: " + consumerRecord.key());
				logger.info("Value: " + consumerRecord.value());

			}
		}

	}

}