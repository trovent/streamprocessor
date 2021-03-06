package com.trovent.streamprocessor.test.esper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.espertech.esper.client.ConfigurationException;
import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EPStatementException;
import com.espertech.esper.client.EventType;
import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.ProducerListener;
import com.trovent.streamprocessor.kafka.StringQueueProducer;

public class TestTSPEngine {

	private TSPEngine engine;

	public TestTSPEngine() {
		super();
	}

	@BeforeEach
	protected void setUp() throws Exception {
		engine = TSPEngine.create();
		engine.init();
	}

	@AfterEach
	protected void tearDown() throws Exception {
		engine.shutdown();
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
	public void testAddListenerFailed() {
		final String STATEMENTNAME = "notExisting";
		assertThrows(EPException.class,
				() -> engine.addListener(STATEMENTNAME, new ProducerListener(new StringQueueProducer())));
	}

	@Test
	public void testAddListener() {
		final String SCHEMANAME = "createmyschema";
		final String STATEMENTNAME = "mystatement";

		engine.addEPLStatement("create map schema inputqueue as (name string, age int)", SCHEMANAME);
		engine.addEPLStatement("select name from inputqueue", STATEMENTNAME);

		engine.addListener(STATEMENTNAME, new ProducerListener(new StringQueueProducer()));
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
	public void testSendEPLEventOBJECTARRAYWithWrongData() {
		String statement;
		statement = "create objectarray schema SomeArrayEventSchema as (first_name string, numbers int)";
		engine.addEPLStatement(statement, "ArraySchema");

		statement = "select numbers as numbers from SomeArrayEventSchema";
		engine.addEPLStatement(statement, "ArrayStatement");

		// testData as objectarray
		Object[] objArrayData = new Object[4];
		objArrayData[0] = new String("Alice");
		objArrayData[1] = new String("fourty-two");
		objArrayData[2] = new Long(33);
		objArrayData[3] = true;

		// TODO reenable this as soon as a fix is found / fix this
		// assertThrows(EPException.class, () ->
		// engine.sendEPLEvent("SomeArrayEventSchema", objArrayData));
	}

	@Test
	public void testSendEPLEventMAPWithWrongDataName() {
		String statement;
		statement = "create map schema SomeMapEventSchema as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, "MapSchema");

		statement = "select first_name as First_Name from SomeMapEventSchema";
		engine.addEPLStatement(statement, "MapStatement");

		Map<String, Object> mapData = new HashMap<String, Object>();
		mapData.put("Bielefeld", 42);
		mapData.put("first_name", "Alice");

		// TODO reenable this as soon as a fix is found / fix this
		// assertThrows(EPException.class, () ->
		// engine.sendEPLEvent("SomeMapEventSchema", mapData));
	}

	@Test
	public void testSendEPLEventOBJECTARRAYToWrongEvent() {
		String statement;
		statement = "create objectarray schema SomeArrayEventSchema as (first_name string, numbers int)";
		engine.addEPLStatement(statement, "ArraySchema");

		statement = "select numbers as numbers from SomeArrayEventSchema";
		engine.addEPLStatement(statement, "ArrayStatement");

		// testData as objectarray
		Object[] objArrayData = new Object[2];
		objArrayData[0] = new String("Alice");
		objArrayData[1] = new Integer(42);

		assertThrows(EPException.class, () -> engine.sendEPLEvent("Bielefeld", objArrayData));
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
		newEventType.put("myBigInteger", "bigInteger");
		newEventType.put("myBigDecimal", "bigDecimal");

		newEventType.put("myDuration", "duration");
		newEventType.put("myLocaltime", "localdate");
		newEventType.put("mylocalDateTime", "localdatetime");
		newEventType.put("myLocalTime", "localtime");
		newEventType.put("myOffsetDateTime", "offsetdatetime");
		newEventType.put("myOffsetTime", "offsettime");
		newEventType.put("myZonedDateTime", "zoneddatetime");

		engine.addEPLSchema("TestEventSchema", newEventType);
	}

	@Test
	public void testAddEPLSchemaWithJavaTypes() {
		Map<String, String> newEventType = new HashMap<String, String>();

		newEventType.put("myPeriod", "java.time.Period");
		newEventType.put("myYear", "java.time.Year");
		newEventType.put("myChar", "java.lang.Character");

		engine.addEPLSchema("TestEvent", newEventType);
	}

	@Test
	public void testAddEPLSchemaStrangeEntries() {
		Map<String, String> newEventType = new HashMap<String, String>();

		newEventType.put("myString", "wooords");
		newEventType.put("foo", "bar");

		assertThrows(EPException.class, () -> engine.addEPLSchema("TestEvent", newEventType));
	}

	@Test
	public void testAddEPLSchemaAsArray() {

		final String SCHEMA_NAME = "TestEventSchema";
		String[] propNames = new String[7];
		String[] typeNames = new String[propNames.length];

		propNames[0] = "myString";
		typeNames[0] = "string";
		propNames[1] = "myInt";
		typeNames[1] = "integer";
		propNames[2] = "myBool";
		typeNames[2] = "boolean";
		propNames[3] = "myFloat";
		typeNames[3] = "float";
		propNames[4] = "myDouble";
		typeNames[4] = "double";
		propNames[5] = "myLong";
		typeNames[5] = "long";
		propNames[6] = "myByte";
		typeNames[6] = "byte";

		engine.addEPLSchema(SCHEMA_NAME, propNames, typeNames);
		EventType eventType = engine.getEPServiceProvider().getEPAdministrator().getConfiguration()
				.getEventType(SCHEMA_NAME);
		assertNotNull(eventType.getPropertyDescriptor("myString"));
		assertNotNull(eventType.getPropertyDescriptor("myByte"));
	}

	@Test
	public void testAddEPLSchemaAsArrayInvalidSize() {

		final String SCHEMA_NAME = "TestEventSchema";
		String[] propNames = new String[3];
		String[] typeNames = new String[propNames.length - 1];

		propNames[0] = "myString";
		typeNames[0] = "string";
		propNames[1] = "myInt";
		typeNames[1] = "integer";
		propNames[2] = "myBool";

		assertThrows(EPException.class, () -> engine.addEPLSchema(SCHEMA_NAME, propNames, typeNames));
	}

	@Test
	public void testAddEPLSchemaAsArrayInvalidTypename() {

		final String SCHEMA_NAME = "TestEventSchema";
		String[] propNames = new String[3];
		String[] typeNames = new String[propNames.length];

		propNames[0] = "myString";
		typeNames[0] = "xyz";
		propNames[1] = "myInt";
		typeNames[1] = "integer";

		assertThrows(EPException.class, () -> engine.addEPLSchema(SCHEMA_NAME, propNames, typeNames));
	}

	@Test
	public void testHasSchemaForSchemaExists() {
		this.testAddEPLSchemaAllowedEntries();
		assertTrue(engine.hasEPLSchema("TestEventSchema"));
	}

	@Test
	public void testHasSchemaForSchemaExistsCreatedByAddEPLStatement() {
		String statement;
		String statementName = "StatementMadeSchema";
		String eventTypeName = "eventName";
		statement = "create objectarray schema " + eventTypeName + " as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, statementName);

		assertFalse(engine.hasEPLSchema(statementName));
		assertFalse(engine.hasStatement(eventTypeName));
		assertTrue(engine.hasEPLSchema(eventTypeName));
		assertTrue(engine.hasStatement(statementName));
	}

	@Test
	public void testHasSchemaForSchemaDoesNotExist() {
		this.testAddEPLSchemaAllowedEntries();
		assertFalse(engine.hasEPLSchema("Bielefeld"));
	}

	@Test
	public void testRemoveEPLSchema() {
		this.testAddEPLSchemaAllowedEntries();
		assertTrue(engine.hasEPLSchema("TestEventSchema"));
		engine.removeEPLSchema("TestEventSchema");
		assertFalse(engine.hasEPLSchema("TestEventSchema"));
	}

	@Test
	public void testRemoveEPLSchemaCreatedByAddEPLStatement() {
		String statement;
		String statementName = "StatementMadeSchema";
		String eventTypeName = "eventName";
		statement = "create objectarray schema " + eventTypeName + " as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, statementName);

		assertTrue(engine.hasEPLSchema(eventTypeName));
		assertThrows(ConfigurationException.class, () -> engine.removeEPLSchema(eventTypeName)); // should throw error
		assertTrue(engine.hasEPLSchema(eventTypeName)); // should still be true
		engine.removeEPLSchema(eventTypeName, true);
		assertFalse(engine.hasEPLSchema(eventTypeName));
		assertTrue(engine.hasStatement(statementName)); // The statement still exists!!
	}

	@Test
	public void testRemoveEPLSchemaCreatedByAddEPLStatementByDeletingStatement() {
		String statement;
		String statementName = "StatementMadeSchema";
		String eventTypeName = "eventName";
		statement = "create objectarray schema " + eventTypeName + " as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, statementName);

		assertTrue(engine.hasEPLSchema(eventTypeName));
		assertTrue(engine.hasStatement(statementName));
		engine.removeEPLStatement(statementName);
		// assertTrue(engine.hasSchema(eventTypeName));
		assertFalse(engine.hasStatement(statementName));
		assertThrows(EPException.class, () -> engine.removeEPLSchema(eventTypeName));
		assertFalse(engine.hasStatement(statementName));
		assertFalse(engine.hasEPLSchema(eventTypeName));
	}

	@Test
	public void testGetEPLSchema() {
		this.testAddEPLSchemaAllowedEntries();
		String eventTypeName = "TestEventSchema"; // from a
		assertEquals(eventTypeName, (engine.getEPLSchema(eventTypeName).name));

		String statement;
		String statementName = "StatementMadeSchema";
		eventTypeName = "secondEvent";

		statement = "create objectarray schema " + eventTypeName + " as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, statementName);

		assertEquals(eventTypeName, (engine.getEPLSchema(eventTypeName).name));
	}

	@Test
	public void testGetEPLSchemaForNonexistantEventType() {
		assertThrows(EPException.class, () -> engine.getEPLSchema("Bielefeld"));
	}

	@Test
	public void testGetEPLSchemas() {
		final String SCHEMANAME = "createmyschema";

		Map<String, String> newEventType = new HashMap<String, String>();

		newEventType.put("myString", "string");
		newEventType.put("myinteger", "integer");
		newEventType.put("myinteger2", "int");

		engine.addEPLSchema(SCHEMANAME, newEventType);
		List<EplSchema> schemalist = engine.getEPLSchemas();

		assertEquals(SCHEMANAME, schemalist.get(0).name);
	}

}
