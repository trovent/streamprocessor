package com.trovent.streamprocessor.test;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.ConnectorController;
import com.trovent.streamprocessor.kafka.ConsumerThread;
import com.trovent.streamprocessor.kafka.KafkaManager;
import com.trovent.streamprocessor.kafka.ProducerListener;
import com.trovent.streamprocessor.kafka.StringQueueConsumer;
import com.trovent.streamprocessor.kafka.StringQueueProducer;
import com.trovent.streamprocessor.restapi.ConsumerConnector;
import com.trovent.streamprocessor.restapi.ProducerConnector;

class TestConnectorController {

	TSPEngine engine = TSPEngine.create();

	KafkaManager kafkaManager;

	StringQueueConsumer consumer;

	String topic = "input";

	EplSchema schema;
	EplStatement statement;

	@BeforeEach
	void setUp() throws Exception {

		kafkaManager = new KafkaManager();

		consumer = new StringQueueConsumer();

		engine.init();

		schema = new EplSchema("employees");
		schema.add("name", "string").add("duration", "integer").add("isMale", "boolean");

		statement = new EplStatement("FilterNewbies", "select name, duration, isMale from employees where duration<4");

		engine.addEPLSchema(schema);
		engine.addEPLStatement(statement);
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testConsumerConnector() throws InterruptedException, JsonParseException, JsonMappingException, IOException {

		// Test: (data) => Consumer => TSPEngine => (result)
		//
		// connect TSPEngine with consumer given by connector
		ConnectorController controller = ConnectorController.create(this.engine, this.kafkaManager);
		ConsumerConnector connector = new ConsumerConnector(null, schema.name);
		int hashCode = controller.connect(connector);

		// get consumer object for debugging purposes
		ConsumerThread cThread = controller.getConsumerThread(hashCode);
		assertNotNull(cThread);
		StringQueueConsumer consumer = (StringQueueConsumer) cThread.getConsumer();
		assertNotNull(consumer);

		// create a listener to read output events
		StringQueueProducer producer = new StringQueueProducer();
		this.engine.addListener(statement.name, new ProducerListener(producer));

		// push event into consumer
		// => will be read be consumerThread
		// => will be put into connected event schema (schema.name)
		EplEvent event = new EplEvent(schema.name).add("name", "John").add("duration", 1).add("isMale", true);
		consumer.push(event.dataToJson());

		while (producer.isEmpty()) {
			Thread.sleep(10);
		}

		String outputEvent = producer.poll();
		EplEvent resultEvent = EplEvent.fromJson(outputEvent);

		assertEquals(event.data, resultEvent.data);
	}

	@Test
	void testConsumerConnectorWithSource()
			throws InterruptedException, JsonParseException, JsonMappingException, IOException {

		// Test: (data) => Consumer => TSPEngine => (result)
		//
		// connect TSPEngine with consumer given by connector
		final String DATASOURCE = "MyData";
		ConnectorController controller = ConnectorController.create(this.engine, this.kafkaManager);
		ConsumerConnector connector = new ConsumerConnector(null, schema.name, DATASOURCE);
		int hashCode = controller.connect(connector);

		// get consumer object for debugging purposes
		ConsumerThread cThread = controller.getConsumerThread(hashCode);
		assertNotNull(cThread);
		StringQueueConsumer consumer = (StringQueueConsumer) cThread.getConsumer();
		assertNotNull(consumer);

		// create a listener to read output events
		StringQueueProducer producer = new StringQueueProducer();
		this.engine.addListener(statement.name, new ProducerListener(producer));

		// push event into consumer
		// => will be read be consumerThread
		// => will be put into connected event schema (schema.name)
		EplEvent event = new EplEvent(schema.name).add("name", "John").add("duration", 1).add("isMale", true);
		consumer.push(event.dataToJson(DATASOURCE));

		while (producer.isEmpty()) {
			Thread.sleep(10);
		}

		String outputEvent = producer.poll();
		EplEvent resultEvent = EplEvent.fromJson(outputEvent);

		assertEquals(event.data, resultEvent.data);
	}

	@Test
	void testProducerConnector() throws InterruptedException, JsonParseException, JsonMappingException, IOException {

		// Test: (data) => TSPEngine => Producer => (result)
		//
		ConnectorController controller = ConnectorController.create(this.engine, this.kafkaManager);

		// on every event in <statement.name>, data is written into producer
		ProducerConnector connector = new ProducerConnector(null, statement.name);
		int hashCode = controller.connect(connector);

		// get listener to check output events
		ProducerListener listener = controller.getListener(hashCode);
		assertNotNull(listener);
		StringQueueProducer producer = (StringQueueProducer) listener.getProducer();
		assertNotNull(producer);

		// now kafka topic "input" is connector to schema "employees"
		// => consumer thread reads data from kafka into esper schema
		EplEvent eventEve = new EplEvent("employees").add("name", "Eve").add("duration", 1).add("isMale", false);
		EplEvent eventBob = new EplEvent("employees").add("name", "Bob").add("duration", 2).add("isMale", true);
		this.engine.sendEPLEvent(eventEve);
		this.engine.sendEPLEvent(eventBob);

		// wait until processed
		while (producer.isEmpty()) {
			Thread.sleep(10);
		}

		// retrieve from StringQueueProducer
		String data = producer.poll();
		assertNotNull(data);

		// compare
		EplEvent result = EplEvent.fromJson(data);
		assertEquals(eventEve.data, result.data);

		// retrieve from StringQueueProducer
		data = producer.poll();
		assertNotNull(data);

		// compare
		result = EplEvent.fromJson(data);
		assertEquals(eventBob.data, result.data);
	}
	
	@Test
	void testProducerConnectorWithDestination() throws InterruptedException, JsonParseException, JsonMappingException, IOException {

		// Test: (data) => TSPEngine => Producer => (result)
		//
		
		String destinationKey = "destination_key";
		
		ConnectorController controller = ConnectorController.create(this.engine, this.kafkaManager);

		// on every event in <statement.name>, data is written into producer
		ProducerConnector connector = new ProducerConnector(null, statement.name, destinationKey);
		int hashCode = controller.connect(connector);

		// get listener to check output events
		ProducerListener listener = controller.getListener(hashCode);
		assertNotNull(listener);
		StringQueueProducer producer = (StringQueueProducer) listener.getProducer();
		assertNotNull(producer);

		// now kafka topic "input" is connector to schema "employees"
		// => consumer thread reads data from kafka into esper schema
		EplEvent eventEve = new EplEvent("employees").add("name", "Eve").add("duration", 1).add("isMale", false);
		this.engine.sendEPLEvent(eventEve);

		// wait until processed
		while (producer.isEmpty()) {
			Thread.sleep(10);
		}

		// retrieve from StringQueueProducer
		String data = producer.poll();
		assertNotNull(data);
		assertTrue("Output json string with data does not have destination key", data.indexOf("\""+destinationKey+"\"") >= 0);
	}

	@Test
	void testConnectorPipeline() throws InterruptedException, IOException {

		// Test: (data) => Consumer => TSPEngine => Producer => (result)
		//
		ConnectorController controller = ConnectorController.create(this.engine, this.kafkaManager);

		ProducerConnector prodConnector = new ProducerConnector(null, statement.name);
		int hashCodeListener = controller.connect(prodConnector);

		ConsumerConnector conConnector = new ConsumerConnector(null, schema.name);
		int hashCodeConsumer = controller.connect(conConnector);

		// --- get internal objects needed for testing ---
		ProducerListener listener = controller.getListener(hashCodeListener);
		assertNotNull(listener);
		StringQueueProducer producer = (StringQueueProducer) listener.getProducer();
		assertNotNull(producer);

		ConsumerThread cThread = controller.getConsumerThread(hashCodeConsumer);
		assertNotNull(cThread);
		StringQueueConsumer consumer = (StringQueueConsumer) cThread.getConsumer();
		assertNotNull(consumer);
		// --- --- ---

		// create testing data
		EplEvent eventEve = new EplEvent("employees").add("name", "Eve").add("duration", 1).add("isMale", false);
		EplEvent eventBob = new EplEvent("employees").add("name", "Bob").add("duration", 2).add("isMale", true);

		assertEquals(0, producer.count());

		// put data into consumer => esper can consumer
		consumer.push(eventEve.dataToJson());

		while (producer.count() < 1) {
			Thread.sleep(10);
		}
		consumer.push(eventBob.dataToJson());

		while (producer.count() < 2) {
			Thread.sleep(10);
		}

		// check results that arrived in the producer
		assertEquals(2, producer.count());
		String data = producer.poll();
		EplEvent event = EplEvent.fromJson(data);
		assertEquals(eventEve.data, event.data);

		data = producer.poll();
		event = EplEvent.fromJson(data);
		assertEquals(eventBob.data, event.data);
	}
}
