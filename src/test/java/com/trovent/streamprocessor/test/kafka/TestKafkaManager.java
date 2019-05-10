package com.trovent.streamprocessor.test.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.trovent.streamprocessor.JSONInputProcessor;
import com.trovent.streamprocessor.esper.BufferedListener;
import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.ConsumerThread;
import com.trovent.streamprocessor.kafka.InputProcessor;
import com.trovent.streamprocessor.kafka.KafkaManager;
import com.trovent.streamprocessor.kafka.StringQueueConsumer;

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
	void testCreateConsumerThread() throws InterruptedException {
		// create input processer which send data to Esper engine
		InputProcessor input = new JSONInputProcessor(this.engine, this.schema.name);
		// create StringQueueConsumer - reading string data from a Queue
		StringQueueConsumer consumer = new StringQueueConsumer();

		ConsumerThread consumerThread = manager.createConsumerThread(consumer, input);

		BufferedListener listener = new BufferedListener();
		this.engine.addListener(this.statement.name, listener);

		// build an event and send push it to consumer
		EplEvent inputEvent = new EplEvent().add("one", "Hello").add("two", 666);
		Gson gson = new Gson();
		consumer.push(gson.toJson(inputEvent.data));

		while (listener.size() == 0) {
			Thread.sleep(100);
		}

		assertEquals(1, listener.size());

		EplEvent outputEvent = listener.peek();
		assertEquals(inputEvent.data.get("one"), outputEvent.data.get("one"));

		consumerThread.stop();
	}
}
