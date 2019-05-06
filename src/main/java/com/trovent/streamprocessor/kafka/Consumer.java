package com.trovent.streamprocessor.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Consumer implements Runnable {

	private Logger logger;

	private String topic;

	private KafkaConsumer<String, String> consumer;

	private InputProcessor input;

	private boolean isStopped = true;

	public Consumer(Properties props, String topic, InputProcessor input) {

		this.topic = topic;
		this.input = input;
		this.logger = LogManager.getLogger();

		this.consumer = new KafkaConsumer<>(props);

		List<String> topics = new ArrayList<>();
		topics.add(this.topic);
		this.consumer.subscribe(topics);

		this.isStopped = false;
	}

	public void start() {

	}

	@Override
	public void run() {

		this.logger.debug(String.format("KafkaConsumer:  starting to read from topic '%s'", this.topic));

		while (!this.isStopped) {
			ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				this.logger.trace("offset: {}  key: {}  value: {}", record.offset(), record.key(), record.value());
				input.process(record.value());
			}
		}

		this.consumer.close();

		this.logger.debug(String.format("KafkaConsumer:  stop reading from topic '%s'", this.topic));
	}
}