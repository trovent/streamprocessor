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

/**
 * 
 * @author tobias nieberg
 * 
 * @brief TSPKafkaConsumer implements a Consumer that reads from a given Kafka
 *        topic (i.e. it acts like a Kafka Consumer). After initialisation with
 *        the constructor, method poll() polls data from a kafka host and
 *        returns them as string array.
 *
 */
public class TSPKafkaConsumer implements IConsumer {

	private Logger logger;

	private String topic;

	private KafkaConsumer<String, String> consumer;

	/**
	 * Create a new TSPKafkaConsumer using the given properties option to create a
	 * new KafkaConsumer object implicitly.
	 * 
	 * @param props Properties passed to the KafkaConsumer constructor
	 * @param topic Topic to read from kafka
	 */
	public TSPKafkaConsumer(Properties props, String topic) {

		init(new KafkaConsumer<String, String>(props), topic);
	}

	/**
	 * Create a new TSPKafkaConsumer with the given KafkaConsumer object.
	 * 
	 * @param kafkaConsumer Existing KafkaConsumer object used to poll data
	 * @param topic         Topic to read from kafka
	 */
	public TSPKafkaConsumer(KafkaConsumer<String, String> kafkaConsumer, String topic) {

		init(kafkaConsumer, topic);
	}

	private void init(KafkaConsumer<String, String> kafkaConsumer, String topic) {

		this.topic = topic;
		this.logger = LogManager.getLogger();

		this.consumer = kafkaConsumer;

		List<String> topics = new ArrayList<>();
		topics.add(this.topic);
		this.consumer.subscribe(topics);

		this.logger.debug(String.format("KafkaConsumer:  starting to read from topic '%s'", this.topic));
	}

	/**
	 * Poll data from a kafka topic and return them as string array. If no data is
	 * received within a certain timespan, an empty array is returned.
	 * 
	 * @param duration time to wait for data in milliseconds
	 * @return Array of strings with data. May be empty.
	 */
	@Override
	public String[] poll(Duration duration) {

		ArrayList<String> data = new ArrayList<>();

		ConsumerRecords<String, String> records = this.consumer.poll(duration);

		for (ConsumerRecord<String, String> record : records) {
			this.logger.trace("offset: {}  key: {}  value: {}", record.offset(), record.key(), record.value());
			data.add(record.value());
		}

		return (String[]) data.toArray();
	}

	protected void finalize() {
		consumer.close();
		this.logger.debug(String.format("KafkaConsumer:  stop reading from topic '%s'", this.topic));
	}

}
