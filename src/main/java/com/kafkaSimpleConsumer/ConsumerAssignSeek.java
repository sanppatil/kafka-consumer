package com.kafkaSimpleConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerAssignSeek {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ConsumerAssignSeek.class);

		String bootstrapServer = "localhost:9092";
		String groupId = "my-kafka-consumer";
		String topic = "fst_topic";

		// Create producer properties
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);


		// Create consumer client
		@SuppressWarnings("resource") // This is suppressed as its demo program
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		// assign and seek are mostly used to reply data or fetch specific message
		TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
		long offsetToReadFrom = 15l;
		consumer.assign(Arrays.asList(partitionToReadFrom));

		// seek
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		int numberOfMsgToRead = 5;
		boolean keepOnReading = true;
		int numberOfMsgReadSoFar = 0;

		// Subscribe consumer to topics
		consumer.subscribe(Collections.singleton(topic));

		// poll for new data
		while (keepOnReading) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				numberOfMsgReadSoFar++;
				logger.info("Topic: " + consumerRecord.topic());
				logger.info("Partition: " + consumerRecord.partition());
				logger.info("Offset: " + consumerRecord.offset());
				logger.info("Key: " + consumerRecord.key());
				logger.info("Value: " + consumerRecord.value());
				if (numberOfMsgReadSoFar > numberOfMsgToRead) {
					keepOnReading = false;
					break;
				}
			}
		}
		
		logger.info("Exiting applicaiton...");

	}

}