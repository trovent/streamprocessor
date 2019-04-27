package com.trovent.streamprocessor.kafka;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaManager {

	private ExecutorService executor;

	private Logger logger;

	public KafkaManager() {

		this.logger = LogManager.getLogger();

		logger.trace("creating executor");
		executor = Executors.newCachedThreadPool();
	}

	/*
	 * Create a KafkaConsumer object with given topic Use the Consumer object to
	 * start a new thread that reads from the given topic and puts data into the
	 * given InputProcessor.
	 */
	public Consumer createConsumer(String topic, InputProcessor input) {

		logger.trace(String.format("creating new consumer for topic '%s'", topic));

		Consumer consumer = new Consumer(topic, input);

		executor.execute(consumer);

		return consumer;
	}

	/*
	 * public KafkaProducer createProducer(String topic) {
	 * 
	 * }
	 */
}
