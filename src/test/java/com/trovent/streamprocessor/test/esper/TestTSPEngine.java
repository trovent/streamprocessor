package com.trovent.streamprocessor.test.esper;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EPStatementException;
import com.trovent.streamprocessor.esper.TSPEngine;

import junit.framework.TestCase;

public class TestTSPEngine extends TestCase {

	private TSPEngine engine;

	public TestTSPEngine() {
		super();
	}

	protected void setUp() throws Exception {
		super.setUp();

		engine = new TSPEngine();
		engine.init();
	}

	protected void tearDown() throws Exception {

		engine.shutdown();
		super.tearDown();
	}

	@Test
	public void testAddEPLStatementCreatingEventType() {

		final String EPLNAME = "createmyschema";
		String result = engine.addEPLStatement("create map schema inputqueue as (name string, age int)", EPLNAME);

		assertNotNull(result);
		assertEquals(EPLNAME, result);
	}

	@Test
	public void testAddEPLStatementCreatingEventTypeTwice() {

		final String EPLNAME = "createmyschema";
		engine.addEPLStatement("create map schema inputqueue as (name string, age int)", EPLNAME);

		assertThrows(EPStatementException.class,
				() -> engine.addEPLStatement("create map schema inputqueue as (phoneno int)", EPLNAME));
	}

	@Test
	public void testCreateStatementWithoutEventType() {

		// EventType 'inputqueue' is not defined => Exception
		assertThrows(EPStatementException.class,
				() -> engine.addEPLStatement("select name from inputqueue", "mystatement"));
	}

	@Test
	public void testAddEPLStatementWithEventType() {

		final String SCHEMANAME = "createmyschema";
		final String STATEMENTNAME = "mystatement";

		engine.addEPLStatement("create map schema inputqueue as (name string, age int)", SCHEMANAME);
		String result = engine.addEPLStatement("select name from inputqueue", STATEMENTNAME);

		assertEquals(STATEMENTNAME, result);
	}

	@Test
	public void testSendEPLEventMAP() {
		String statement;
		statement = "create map schema SomeMapEventSchema as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, "MapSchema");

		statement = "select first_name as First_Name from SomeMapEventSchema";
		engine.addEPLStatement(statement, "MapStatement");

		Map<String, Object> mapData = new HashMap<String, Object>();
		mapData.put("numbers", 42);
		mapData.put("first_name", "Alice");

		engine.sendEPLEvent("SomeMapEventSchema", mapData);
	}

	@Test
	public void testSendEPLEventOBJECTARRAY() {
		String statement;
		statement = "create objectarray schema SomeArrayEventSchema as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, "ArraySchema");

		statement = "select first_name as First_Name from SomeArrayEventSchema";
		engine.addEPLStatement(statement, "ArrayStatement");

		// testData as objectarray
		Object[] objArrayData = new Object[2];
		objArrayData[0] = new String("Alice");
		objArrayData[1] = new Integer(42);

		engine.sendEPLEvent("SomeArrayEventSchema", objArrayData);
	}

	@Test
	public void testAddEPLSchemaAllowedEntries() {
		Map<String, String> newEventType = new HashMap<String, String>();

		newEventType.put("myString", "string");
		newEventType.put("myinteger", "integer");
		newEventType.put("myinteger2", "int");
		newEventType.put("myboolean", "boolean");
		newEventType.put("myLong", "long");
		newEventType.put("myDouble", "double");
		newEventType.put("myFloat", "float");
		newEventType.put("myByte", "byte");

		engine.addEPLSchema("TestEventSchema", newEventType);
	}

	@Test
	public void testAddEPLSchemaStrangeEntries() {
		Map<String, String> newEventType = new HashMap<String, String>();

		newEventType.put("myString", "wooords");
		newEventType.put("foo", "bar");

		assertThrows(EPException.class, () -> engine.addEPLSchema("TestEvent", newEventType));
	}

	@Test
	public void testHasSchemaForSchemaExists() {
		this.testAddEPLSchemaAllowedEntries();
		assertTrue(engine.hasSchema("TestEventSchema"));
	}

	@Test
	public void testHasSchemaForSchemaExistsCreatedByAddEPLStatement() {
		String statement;
		String statementName = "StatementMadeSchema";
		String eventTypeName = "eventName";
		statement = "create objectarray schema " + eventTypeName + " as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, statementName);

		assertFalse(engine.hasSchema(statementName));
		assertFalse(engine.hasStatement(eventTypeName));
		assertTrue(engine.hasSchema(eventTypeName));
		assertTrue(engine.hasStatement(statementName));
	}

	@Test
	public void testHasSchemaForSchemaDoesNotExist() {
		this.testAddEPLSchemaAllowedEntries();
		assertFalse(engine.hasSchema("Bielefeld"));
	}

	@Test
	public void testRemoveEPLSchema() {
		this.testAddEPLSchemaAllowedEntries();
		assertTrue(engine.hasSchema("TestEventSchema"));
		engine.removeEPLSchema("TestEventSchema");
		assertFalse(engine.hasSchema("TestEventSchema"));
	}

	@Test
	public void testRemoveEPLSchemaCreatedByAddEPLStatement() {
		String statement;
		String statementName = "StatementMadeSchema";
		String eventTypeName = "eventName";
		statement = "create objectarray schema " + eventTypeName + " as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, statementName);

		assertTrue(engine.hasSchema(eventTypeName));
		engine.removeEPLSchema(eventTypeName); // i know this fails
		assertFalse(engine.hasSchema(eventTypeName));
	}

	@Test
	public void testRemoveEPLSchemaCreatedByAddEPLStatementInTwoSteps() {
		String statement;
		String statementName = "StatementMadeSchema";
		String eventTypeName = "eventName";
		statement = "create objectarray schema " + eventTypeName + " as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, statementName);

		assertTrue(engine.hasSchema(eventTypeName));
		assertTrue(engine.hasStatement(statementName));
		engine.removeEPLStatement(statementName);
		// assertTrue(engine.hasSchema(eventTypeName));
		assertFalse(engine.hasStatement(statementName));
		assertThrows(EPException.class, () -> engine.removeEPLSchema(eventTypeName));
		assertFalse(engine.hasStatement(statementName));
		assertFalse(engine.hasSchema(eventTypeName));
	}

}
