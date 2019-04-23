package com.trovent.streamprocessor.test.esper;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.espertech.esper.client.EPStatementException;
import com.trovent.streamprocessor.TSPEngine;

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
		
		//testData as objectarray
		Object[] objArrayData = new Object[2];
		objArrayData[0] = new String("Alice");
		objArrayData[1] = new Integer(42);
		
		engine.sendEPLEvent("SomeArrayEventSchema", objArrayData);
	}


}
