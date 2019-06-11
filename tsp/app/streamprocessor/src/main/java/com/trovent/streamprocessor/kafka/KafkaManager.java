package com.trovent.streamprocessor.kafka;

import java.io.IOException;
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

	public KafkaManager(String configFileLocation) {
		this.init(loadProperties(configFileLocation));
	}
	
	public KafkaManager() {
		this.init(null);
	}

	public void init(Properties properties) {

		this.logger = LogManager.getLogger();

		// use given props
		this.props = properties;

		// if no props given...
		if (this.props == null) {
			this.props = new Properties();
			// or use default config
			logger.warn("No kafka properties given! Setting some default values");
			props.put("bootstrap.servers", "localhost:29092");
			props.put("group.id", "test");
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		}
		this.props.forEach((key, value) -> {
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
	public ConsumerThread createConsumerThread(String topic, InputProcessor input) {

		logger.trace(String.format("creating new consumer for topic '%s'", topic));

		ConsumerThread consumerThread = new ConsumerThread(props, topic, input);

		executor.execute(consumerThread);

		return consumerThread;
	}

	public ConsumerThread createConsumerThread(IConsumer consumer, InputProcessor input) {

		logger.trace(String.format("creating new consumer"));

		ConsumerThread consumerThread = new ConsumerThread(consumer, input);

		executor.execute(consumerThread);

		return consumerThread;
	}

	public TSPKafkaProducer createProducer(String topic) {
		return new TSPKafkaProducer(this.props, topic);
	}
	
	private Properties loadProperties(String configFileLocation) {
		Properties props = new Properties();
		try {
			props.load(this.getClass().getClassLoader().getResourceAsStream(configFileLocation));
		} catch (IOException e) {
			props = null;
		} catch (Exception e) {
			props = null;
		}
		return props;
	}
}
