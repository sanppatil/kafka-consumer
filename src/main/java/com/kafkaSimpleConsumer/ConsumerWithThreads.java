package com.kafkaSimpleConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWithThreads {

	private Logger logger = LoggerFactory.getLogger(ConsumerWithThreads.class);

	private ConsumerWithThreads() {
	}

	public static void main(String[] args) {
		new ConsumerWithThreads().run();
	}

	private void run() {
		String bootstrapServer = "localhost:9092";
		String groupId = "my-kafka-consumer";
		String topic = "fst_topic";

		// latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);

		// Create consumer runnable
		logger.info("Creating consumer threads...");
		Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootstrapServer, topic, groupId);

		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();

		// Add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caughut shutdown hook...");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Application has exited...");
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted...", e);
		} finally {
			logger.info("Application is closing...");
		}
	}

	public class ConsumerRunnable implements Runnable {

		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		public ConsumerRunnable(CountDownLatch latch, String bootstrapServer, String topic, String groupId) {
			this.latch = latch;

			// Create consumer properties
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			// Create consumer client
			consumer = new KafkaConsumer<>(props);

			// Subscribe consumer to topics
			consumer.subscribe(Collections.singleton(topic));
		}

		@Override
		public void run() {
			// poll for new data
			try {
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
			} catch (WakeupException wakeUpException) {
				logger.info("Recevied shutdown signal...");
			} finally {
				consumer.close();
				latch.countDown(); // tell main code we are done with consumer
			}

		}

		public void shutdown() {
			// This method interrupt consumer.poll()
			// It will throw WakeupException
			consumer.wakeup();
		}

	}

}