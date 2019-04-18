package com.trovent.streamprocessor.test.esper;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.Test;

import com.espertech.esper.client.EPStatementException;
import com.trovent.streamprocessor.TSPEngine;

import junit.framework.TestCase;

public class TestTSPEngine extends TestCase {

	private TSPEngine engine;

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

		engine.addEPLStatement("create map schema inputqueue as (name string, age int)", "createmyschema");
		engine.addEPLStatement("select name from inputqueue", "mystatement");
	}

}
