package com.trovent.streamprocessor.test.esper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.espertech.esper.client.EPException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.trovent.streamprocessor.JSONInputProcessor;
import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.ProducerListener;
import com.trovent.streamprocessor.kafka.StringQueueProducer;

public class TestJSONInputProcessor {

	private TSPEngine engine;

	final String DEFAULT_SCHEMA = "myschema";

	@BeforeEach
	protected void setUp() throws Exception {
		engine = TSPEngine.create();
		engine.init();

		Map<String, String> schema = new HashMap<String, String>();
		schema.put("name", "string");
		schema.put("age", "integer");
		schema.put("isAdult", "boolean");
		engine.addEPLSchema(DEFAULT_SCHEMA, schema);
	}

	@AfterEach
	protected void tearDown() throws Exception {
		engine.shutdown();
	}

	@Test
	public void testCreateProcessor() {
		JSONInputProcessor input = new JSONInputProcessor(engine);
		assertEquals(engine, input.getEngine());
	}

	@Test
	public void testSetEventTypeWithNonExistingEventType() {
		JSONInputProcessor input = new JSONInputProcessor(engine);
		assertThrows(EPException.class, () -> input.setEventType("NonExisting"));
	}

	@Test
	public void testSetEventType() {
		JSONInputProcessor input = new JSONInputProcessor(engine);
		// shall not throw exception
		input.setEventType(DEFAULT_SCHEMA);

		assertEquals(DEFAULT_SCHEMA, input.getEventType().getName());
	}

	@Test
	public void testCreateProcessorWithNotExistingEventType() {
		assertThrows(EPException.class, () -> new JSONInputProcessor(engine, "NonExisting"));
	}

	@Test
	public void testCreateProcessorWithExistingEventType() {

		JSONInputProcessor input = new JSONInputProcessor(engine, DEFAULT_SCHEMA);

		assertEquals(DEFAULT_SCHEMA, input.getEventType().getName());
	}

	@Test
	public void testProcessWithJSONInput()
			throws JsonParseException, JsonMappingException, IOException, InterruptedException {
		JSONInputProcessor input = new JSONInputProcessor(engine, DEFAULT_SCHEMA);

		// create statement, add to engine
		final String STMT_NAME = "MyStatement";
		String stmt = "select *, sum(age), count(*) from " + DEFAULT_SCHEMA;
		this.engine.addEPLStatement(stmt, STMT_NAME);

		StringQueueProducer producer = new StringQueueProducer();
		this.engine.addListener(STMT_NAME, new ProducerListener(producer));
		assertEquals(0, producer.count());

		// create data in JSON format
		String jsonData = "{ \"name\" : \"MyName\", \"age\" : 42, \"isAdult\" : true }";
		assertTrue(input.process(jsonData));

		// wait for listener
		while (producer.isEmpty()) {
			Thread.sleep(10);
		}

		EplEvent event = EplEvent.fromJson(producer.poll());

		// check for 5 field in resulting event
		assertEquals(5, event.data.keySet().size());
	}

	@Test
	public void testProcessWithMalformedJSONInput() {
		JSONInputProcessor input = new JSONInputProcessor(engine, DEFAULT_SCHEMA);

		// create statement, add to engine
		final String STMT_NAME = "MyStatement";
		String stmt = "select *, sum(age), count(*) from " + DEFAULT_SCHEMA;
		this.engine.addEPLStatement(stmt, STMT_NAME);

		// create malformed data in JSON format
		String data = "{ \"name\" : \"MyName, }";
		assertFalse(input.process(data));
	}

	@Test
	public void testProcessWithAllTypes()
			throws InterruptedException, JsonParseException, JsonMappingException, IOException {
		final String SCHEMA = "AllTypes";

		Map<String, String> schema = new HashMap<String, String>();
		schema.put("name", "string");
		schema.put("age", "integer");
		schema.put("isAdult", "boolean");
		schema.put("average", "float");
		schema.put("ratio", "double");
		schema.put("distance", "long");
		schema.put("character", "byte");
		schema.put("Hash", "BigInteger");
		schema.put("HashDec", "BigDecimal");
		engine.addEPLSchema(SCHEMA, schema);

		JSONInputProcessor input = new JSONInputProcessor(engine, SCHEMA);

		// create statement, add to engine
		final String STMT_NAME = "MyStatement";
		String stmt = "select *, sum(age) from " + SCHEMA;
		this.engine.addEPLStatement(stmt, STMT_NAME);

		// add listener to statement
		StringQueueProducer producer = new StringQueueProducer();
		this.engine.addListener(STMT_NAME, new ProducerListener(producer));

		assertEquals(0, producer.count());

		// create data in JSON format
		String data = "{ \"name\" : \"MyName\", \"age\" : 42, "
				+ " \"isAdult\" : \"true\", \"average\" : 3.14, \"ratio\" : 12.3456789, "
				+ " \"distance\" : 987654321, " + " \"character\" : 127, "
				+ "\"Hash\" : 11112222333344445555666677778888, " + " \"HashDec\" : 987654321.987654321" + "}";
		assertTrue(input.process(data));

		// wait for listener
		while (producer.isEmpty()) {
			Thread.sleep(10);
		}

		EplEvent event = EplEvent.fromJson(producer.poll());

		// check for 5 field in resulting event
		assertEquals(10, event.data.keySet().size());
	}

	@Test
	public void testProcessInNestedStructure()
			throws JsonParseException, JsonMappingException, IOException, InterruptedException {

		final String DATAKEY = "MyData";

		JSONInputProcessor input = new JSONInputProcessor(engine, DEFAULT_SCHEMA, DATAKEY);

		// create statement, add to engine
		final String STMT_NAME = "MyStatement";
		String stmt = "select *, sum(age), count(*) from " + DEFAULT_SCHEMA;
		this.engine.addEPLStatement(stmt, STMT_NAME);

		StringQueueProducer producer = new StringQueueProducer();
		this.engine.addListener(STMT_NAME, new ProducerListener(producer));
		assertEquals(0, producer.count());

		// create data in JSON format
		String jsonData = "{ \"key\" : \"myvalue\", \"" + DATAKEY
				+ "\" : { \"name\" : \"MyName\", \"age\" : 42, \"isAdult\" : true } }";
		assertTrue(input.process(jsonData));

		// wait for listener
		while (producer.isEmpty()) {
			Thread.sleep(10);
		}

		EplEvent event = EplEvent.fromJson(producer.poll());

		// check for 5 field in resulting event
		assertEquals(5, event.data.keySet().size());
	}
}
