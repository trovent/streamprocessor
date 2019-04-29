package com.trovent.streamprocessor.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaManager {
	final String CONFIGFILE = "kafka.properties";

	private ExecutorService executor;

	private Logger logger;

	private Properties props;

	public KafkaManager() {

		this.init();
		// this.loader = loader;

	}

	public void init() {
		this.logger = LogManager.getLogger();
		this.props = new Properties();

		try {
			InputStream instream = this.getClass().getClassLoader().getResourceAsStream(CONFIGFILE);
			if (instream == null) {
				throw new IOException("Kafka config file not found");
			}
			props.load(instream);
			logger.info("loaded kafka config from {}:", CONFIGFILE);
		} catch (IOException e) {
			logger.warn("No kafka.properties file found. Using default config:");
			props.put("bootstrap.servers", "localhost:29092");
			props.put("group.id", "test");
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		}

		props.forEach((key, value) -> {
			logger.info("    {} : {}", key, value);
		});

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

		Consumer consumer = new Consumer(props, topic, input);

		executor.execute(consumer);

		return consumer;
	}

	public Producer createProducer(String topic) {
		return new Producer(this.props, topic);
	}
}
