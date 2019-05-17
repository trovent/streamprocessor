package com.trovent.streamprocessor.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TSPKafkaProducer implements IProducer {
	private Logger logger;

	private String topic;

	private KafkaProducer<String, String> producer;

	public String getTopic() {
		return this.topic;
	}

	public TSPKafkaProducer(Properties props, String topic) {
		this.topic = topic;
		this.logger = LogManager.getLogger();

		this.producer = new KafkaProducer<>(props);
	}

	public void send(String key, String value) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, key, value);

		this.logger.trace("topic: {}  key: {}  value: {}", this.topic, key, value);
		this.producer.send(record);
	}

	public void send(String value) {
		this.send(null, value);
	}
}
