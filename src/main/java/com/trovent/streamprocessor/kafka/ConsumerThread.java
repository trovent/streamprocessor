package com.trovent.streamprocessor.kafka;

import java.time.Duration;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConsumerThread implements Runnable {

	private Logger logger;

	private IConsumer consumer;

	private InputProcessor input;

	private boolean isStopped = true;

	public ConsumerThread(Properties props, String topic, InputProcessor input) {

		init(new TSPKafkaConsumer(props, topic), input);
	}

	public ConsumerThread(IConsumer consumer, InputProcessor input) {

		init(consumer, input);
	}

	public IConsumer getConsumer() {
		return this.consumer;
	}

	private void init(IConsumer consumer, InputProcessor input) {

		this.input = input;
		this.logger = LogManager.getLogger();

		this.consumer = consumer;

		this.isStopped = false;
	}

	public void stop() {
		this.isStopped = true;
	}

	@Override
	public void run() {

		while (!this.isStopped) {
			String[] records = this.consumer.poll(Duration.ofMillis(100));
			for (String record : records) {
				this.logger.trace("value: {}", record);
				input.process(record);
			}
		}
	}
}
