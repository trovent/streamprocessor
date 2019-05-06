package com.trovent.streamprocessor.test.esper;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.trovent.streamprocessor.JSONInputProcessor;
import com.trovent.streamprocessor.esper.TSPEngine;

import junit.framework.TestCase;

public class TestJSONInputProcessor extends TestCase {

	private TSPEngine engine;

	final String DEFAULT_SCHEMA = "myschema";

	protected void setUp() throws Exception {
		engine = TSPEngine.create();
		engine.init();

		Map<String, String> schema = new HashMap<String, String>();
		schema.put("name", "string");
		schema.put("age", "integer");
		schema.put("isAdult", "boolean");
		engine.addEPLSchema(DEFAULT_SCHEMA, schema);
	}

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
	public void testProcessWithMapFailed() {
		JSONInputProcessor input = new JSONInputProcessor(engine, DEFAULT_SCHEMA);

		Map<String, String> data = new HashMap<String, String>();
		data.put("nameX", "MyName");
		data.put("age", "42");
		data.put("isAdult", "true");

		assertFalse(input.process(data));
	}

	@Test
	public void testProcessWithMapSuccess() {
		JSONInputProcessor input = new JSONInputProcessor(engine, DEFAULT_SCHEMA);

		Map<String, String> data = new HashMap<String, String>();
		data.put("name", "John Doe");
		data.put("age", "33");
		data.put("isAdult", "true");

		assertTrue(input.process(data));
	}

	@Test
	public void testProcessWithMapOnStatement() {
		JSONInputProcessor input = new JSONInputProcessor(engine, DEFAULT_SCHEMA);

		Map<String, String> data = new HashMap<String, String>();
		data.put("name", "MyName");
		data.put("age", "42");
		data.put("isAdult", "true");

		final String STMT_NAME = "MyStatement";
		String stmt = "select *, sum(age), count(*) from " + DEFAULT_SCHEMA;
		this.engine.addEPLStatement(stmt, STMT_NAME);

		EPStatement epStatement = this.engine.getEPServiceProvider().getEPAdministrator().getStatement(STMT_NAME);

		epStatement.addListener((newData, oldData) -> {
			System.out.println("Listener");
			EventBean eb = newData[0];
			for (String propName : eb.getEventType().getPropertyNames()) {
				System.out.println(String.format("  %s : %s", propName, eb.get(propName)));
			}

		});

		assertTrue(input.process(data));
	}

	@Test
	public void testProcessWithTypeConversionError() {
		JSONInputProcessor input = new JSONInputProcessor(engine, DEFAULT_SCHEMA);

		// create statement, add to engine
		final String STMT_NAME = "MyStatement";
		String stmt = "select *, sum(age), count(*) from " + DEFAULT_SCHEMA;
		this.engine.addEPLStatement(stmt, STMT_NAME);

		// create malformed data in JSON format
		Map<String, String> data = new HashMap<String, String>();
		data.put("name", "John Doe");
		data.put("age", "abc");
		data.put("isAdult", "true");
		assertFalse(input.process(data));
	}

	@Test
	public void testProcessWithJSONInput() {
		JSONInputProcessor input = new JSONInputProcessor(engine, DEFAULT_SCHEMA);

		// create statement, add to engine
		final String STMT_NAME = "MyStatement";
		String stmt = "select *, sum(age), count(*) from " + DEFAULT_SCHEMA;
		this.engine.addEPLStatement(stmt, STMT_NAME);

		// create listener with tests
		class MyListener implements UpdateListener {
			Boolean isDone = false;
			int length = 0;

			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				this.length = newEvents[0].getEventType().getPropertyNames().length;
				this.isDone = true;
			}
		}
		// add listener to statement
		MyListener listener = new MyListener();
		this.engine.addListener(STMT_NAME, listener);
		assertEquals(0, listener.length);

		// create data in JSON format
		String data = "{ \"name\" : \"MyName\", \"age\" : 42, \"isAdult\" : \"true\" }";
		assertTrue(input.process(data));

		while (!listener.isDone)
			;
		assertEquals(5, listener.length);
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
	public void testProcessWithAllTypes() {
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

		// create listener with tests
		class MyListener implements UpdateListener {
			Boolean isDone = false;
			int length = 0;

			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				this.length = newEvents[0].getEventType().getPropertyNames().length;
				this.isDone = true;
			}
		}
		// add listener to statement
		MyListener listener = new MyListener();
		this.engine.addListener(STMT_NAME, listener);
		assertEquals(0, listener.length);

		// create data in JSON format
		String data = "{ \"name\" : \"MyName\", \"age\" : 42, "
				+ " \"isAdult\" : \"true\", \"average\" : 3.14, \"ratio\" : 12.3456789, "
				+ " \"distance\" : 987654321, " + " \"character\" : 127, "
				+ "\"Hash\" : 11112222333344445555666677778888, " + " \"HashDec\" : 987654321.987654321" + "}";
		assertTrue(input.process(data));

		while (!listener.isDone)
			;
		assertEquals(10, listener.length);
	}
}