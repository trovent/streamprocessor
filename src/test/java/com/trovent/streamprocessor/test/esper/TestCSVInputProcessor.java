package com.trovent.streamprocessor.test.esper;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.Test;

import com.espertech.esper.client.EPException;
import com.trovent.streamprocessor.CSVInputProcessor;
import com.trovent.streamprocessor.JSONInputProcessor;
import com.trovent.streamprocessor.esper.TSPEngine;

import junit.framework.TestCase;

public class TestCSVInputProcessor extends TestCase {

	private TSPEngine engine;

	final String DEFAULT_SCHEMA = "myschema";

	protected void setUp() throws Exception {
		engine = TSPEngine.create();
		engine.init();

		String[] propNames = new String[9];
		String[] typeNames = new String[propNames.length];

		propNames[0] = "name";
		typeNames[0] = "string";
		propNames[1] = "age";
		typeNames[1] = "integer";
		propNames[2] = "isAdult";
		typeNames[2] = "boolean";
		propNames[3] = "distance";
		typeNames[3] = "long";
		propNames[4] = "character";
		typeNames[4] = "byte";
		propNames[5] = "average";
		typeNames[5] = "float";
		propNames[6] = "ratio";
		typeNames[6] = "double";
		propNames[7] = "Hash";
		typeNames[7] = "BigInteger";
		propNames[8] = "HashDec";
		typeNames[8] = "BigDecimal";

		engine.addEPLSchema(DEFAULT_SCHEMA, propNames, typeNames);
	}

	protected void tearDown() throws Exception {
		engine.shutdown();
	}

	@Test
	public void testCreateProcessor() {
		CSVInputProcessor input = new CSVInputProcessor(engine);
		assertEquals(engine, input.getEngine());
	}

	@Test
	public void testSetEventTypeWithNonExistingEventType() {
		CSVInputProcessor input = new CSVInputProcessor(engine);
		assertThrows(EPException.class, () -> input.setEventType("NonExisting"));
	}

	@Test
	public void testSetEventType() {
		CSVInputProcessor input = new CSVInputProcessor(engine);
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
		CSVInputProcessor input = new CSVInputProcessor(engine, DEFAULT_SCHEMA);

		assertEquals(DEFAULT_SCHEMA, input.getEventType().getName());
	}

	@Test
	public void testProcessWithArrayFailed() {
		CSVInputProcessor input = new CSVInputProcessor(engine, DEFAULT_SCHEMA);

		String[] data = new String[2];
		data[0] = "John";
		data[1] = "42";

		assertFalse(input.process(data));
	}

	@Test
	public void testProcessWithArraySuccess() {
		CSVInputProcessor input = new CSVInputProcessor(engine, DEFAULT_SCHEMA);

		String[] data = new String[9];
		data[0] = "John";
		data[1] = "42";
		data[2] = "true";
		data[3] = "16123456";
		data[4] = "127";
		data[5] = "3.14159";
		data[6] = "123456.765432";
		data[7] = "123456765432";
		data[8] = "99999.74565665432";

		assertTrue(input.process(data));
	}

	@Test
	public void testProcessWithCSVString() {
		CSVInputProcessor input = new CSVInputProcessor(engine, DEFAULT_SCHEMA);

		String data = "MyName;42;true;947875;-128;1234.665;98765.98765;9876543210000;56789.111222333444555";
		assertTrue(input.process(data));
	}

	@Test
	public void testProcessWithTypeError() {
		CSVInputProcessor input = new CSVInputProcessor(engine, DEFAULT_SCHEMA);

		String[] data = new String[9];
		data[0] = "John";
		data[1] = "42";
		data[2] = "true";
		data[3] = "xyz";
		data[4] = "127";
		data[5] = "3.14159";
		data[6] = "123456.765432";
		data[7] = "123456765432";
		data[8] = "99999.74565665432";

		assertFalse(input.process(data));
	}

}
