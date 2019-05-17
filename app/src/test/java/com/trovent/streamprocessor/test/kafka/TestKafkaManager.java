package com.trovent.streamprocessor.test.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trovent.streamprocessor.JSONInputProcessor;
import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.ConsumerThread;
import com.trovent.streamprocessor.kafka.InputProcessor;
import com.trovent.streamprocessor.kafka.KafkaManager;
import com.trovent.streamprocessor.kafka.ProducerListener;
import com.trovent.streamprocessor.kafka.StringQueueConsumer;
import com.trovent.streamprocessor.kafka.StringQueueProducer;

class TestKafkaManager {

	KafkaManager manager;
	private TSPEngine engine;
	EplSchema schema;
	EplStatement statement;

	@BeforeEach
	void setUp() throws Exception {
		this.engine = TSPEngine.create();
		this.engine.init();
		this.manager = new KafkaManager();

		this.schema = new EplSchema("InputQueue");
		this.schema.add("one", "string").add("two", "integer");

		this.statement = new EplStatement("MyStatement", "select * from InputQueue");

		this.engine.addEPLSchema(this.schema);
		this.engine.addEPLStatement(this.statement);
	}

	@AfterEach
	void tearDown() throws Exception {
		this.manager = null;
		this.engine.shutdown();
	}

	@Test
	void testCreateConsumerThread() throws InterruptedException, IOException {
		// create input processer which send data to Esper engine
		InputProcessor input = new JSONInputProcessor(this.engine, this.schema.name);
		// create StringQueueConsumer - reading string data from a Queue
		StringQueueConsumer consumer = new StringQueueConsumer();

		ConsumerThread consumerThread = manager.createConsumerThread(consumer, input);

		StringQueueProducer producer = new StringQueueProducer();
		this.engine.addListener(this.statement.name, new ProducerListener(producer));

		// build an event and send push it to consumer
		EplEvent inputEvent = new EplEvent().add("one", "Hello").add("two", 666);
		ObjectMapper jackson = new ObjectMapper();
		consumer.push(jackson.writeValueAsString(inputEvent.data));

		while (producer.isEmpty()) {
			Thread.sleep(100);
		}

		assertEquals(1, producer.count());

		String output = producer.poll();
		EplEvent resultEvent = jackson.readValue(output, EplEvent.class);
		assertEquals(inputEvent.data, resultEvent.data);

		consumerThread.stop();
	}
}
