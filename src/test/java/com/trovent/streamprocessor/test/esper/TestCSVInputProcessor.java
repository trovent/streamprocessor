package com.trovent.streamprocessor.test.esper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.espertech.esper.client.EPException;
import com.trovent.streamprocessor.CSVInputProcessor;
import com.trovent.streamprocessor.JSONInputProcessor;
import com.trovent.streamprocessor.esper.TSPEngine;

class TestCSVInputProcessor {

	private TSPEngine engine;

	final String DEFAULT_SCHEMA = "myschema";

	@BeforeEach
	void setUp() throws Exception {
		engine = new TSPEngine();
		engine.init();

		Map<String, String> schema = new HashMap<String, String>();
		schema.put("name", "string");
		schema.put("age", "integer");
		schema.put("isAdult", "boolean");
		schema.put("distance", "long");
		schema.put("character", "byte");
		schema.put("average", "float");
		schema.put("ratio", "double");
		// schema.put("Hash", "BigInteger");
		// schema.put("HashDec", "BigDecmal");

		engine.addEPLSchema(DEFAULT_SCHEMA, schema);
	}

	@AfterEach
	void tearDown() throws Exception {
		engine.shutdown();
	}

	@Test
	void testCreateProcessor() {
		CSVInputProcessor input = new CSVInputProcessor(engine);
		assertEquals(engine, input.getEngine());
	}

	@Test
	void testSetEventTypeWithNonExistingEventType() {
		CSVInputProcessor input = new CSVInputProcessor(engine);
		assertThrows(EPException.class, () -> input.setEventType("NonExisting"));
	}

	@Test
	void testSetEventType() {
		CSVInputProcessor input = new CSVInputProcessor(engine);
		// shall not throw exception
		input.setEventType(DEFAULT_SCHEMA);

		assertEquals(DEFAULT_SCHEMA, input.getEventType().getName());
	}

	@Test
	void testCreateProcessorWithNotExistingEventType() {
		assertThrows(EPException.class, () -> new JSONInputProcessor(engine, "NonExisting"));
	}

	@Test
	void testCreateProcessorWithExistingEventType() {
		CSVInputProcessor input = new CSVInputProcessor(engine, DEFAULT_SCHEMA);

		assertEquals(DEFAULT_SCHEMA, input.getEventType().getName());
	}

	@Test
	void testProcessWithArrayFailed() {
		CSVInputProcessor input = new CSVInputProcessor(engine, DEFAULT_SCHEMA);

		String[] data = new String[2];
		data[0] = "John";
		data[1] = "42";

		assertFalse(input.process(data));
	}

	@Test
	void testProcessWithArraySuccess() {
		CSVInputProcessor input = new CSVInputProcessor(engine, DEFAULT_SCHEMA);

		String[] data = new String[7];
		data[0] = "John";
		data[1] = "42";
		data[2] = "true";
		data[3] = "16123456";
		data[4] = "127";
		data[5] = "3.14159";
		data[6] = "123456.765432";

		assertTrue(input.process(data));
	}

	@Test
	void testProcessWithCSVString() {
		CSVInputProcessor input = new CSVInputProcessor(engine, DEFAULT_SCHEMA);

		String data = "MyName;42;true;947875;-128;1234.665;98765.98765";
		assertTrue(input.process(data));
	}

	@Test
	void testProcessWithTypeError() {
		CSVInputProcessor input = new CSVInputProcessor(engine, DEFAULT_SCHEMA);

		String[] data = new String[7];
		data[0] = "John";
		data[1] = "42";
		data[2] = "true";
		data[3] = "xyz";
		data[4] = "127";
		data[5] = "3.14159";
		data[6] = "123456.765432";

		assertFalse(input.process(data));
	}

}
