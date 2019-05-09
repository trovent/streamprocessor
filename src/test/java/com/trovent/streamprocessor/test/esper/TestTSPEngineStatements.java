package com.trovent.streamprocessor.test.esper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;

public class TestTSPEngineStatements {

	private TSPEngine engine;

	@BeforeEach
	protected void setUp() throws Exception {

		engine = TSPEngine.create();
		engine.init();

		// creates a
		String statement;
		statement = "create map schema SomeMapEventSchema as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, "MapSchema");

		statement = "select count(first_name) as cntFirst_Name from SomeMapEventSchema";
		engine.addEPLStatement(statement, "MapStatement");
	}

	@AfterEach
	protected void tearDown() throws Exception {

		engine.shutdown();
	}

	public void testStartEPLStatement() {
		engine.startEPLStatement("MapStatement");
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapStatement").isStarted());
	}

	public void testStartEPLStatementSCHEMA() {
		engine.stopEPLStatement("MapSchema");
		assertFalse(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapSchema").isStarted());

		engine.startEPLStatement("MapSchema");
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapSchema").isStarted());
	}

	public void testStartEPLStatementForNonexistantStatement() {
		assertThrows(EPException.class, () -> engine.startEPLStatement("Bielefeld"));
	}

	public void testStopEPLStatement() {
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapStatement").isStarted());
		engine.stopEPLStatement("MapStatement");
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapStatement").isStopped());
	}

	public void testStopEPLStatementSCHEMA() {
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapSchema").isStarted());
		engine.stopEPLStatement("MapSchema");
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapSchema").isStopped());
	}

	public void testStopEPLStatementForNonexistantStatement() {
		assertThrows(EPException.class, () -> engine.stopEPLStatement("Bielefeld"));
	}

	// TODO
	public void testRemoveEPLStatement() {
		engine.removeEPLStatement("MapStatement");
	}

	@Test // TODO
	public void testRemoveEPLStatementSCHEMA() {
		engine.removeEPLStatement("MapSchema");
	}

	@Test
	public void testRemoveEPLStatementForNonexistantStatement() {
		assertThrows(EPException.class, () -> engine.removeEPLStatement("Bielefeld"));
	}

	@Test
	public void testAddListenerToStatement() throws InterruptedException {
		class MyListener implements UpdateListener {
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				// does Nothing
			}
		}
		MyListener newListener = new MyListener();
		engine.addListener("MapStatement", newListener);
	}

	@Test
	public void testAddListenerToNonexistantStatement() {
		class MyListener implements UpdateListener {
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				// does Nothing
			}
		}
		assertThrows(EPException.class, () -> engine.addListener("Bielefeld", new MyListener()));
	}

	@Test
	public void testHasStatementForStatementExists() {
		String statement;
		String statementName = "ArraySchema";
		statement = "create objectarray schema SomeArrayEventSchema as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, statementName);

		assertTrue(engine.hasStatement(statementName));
	}

	@Test
	public void testHasStatementForStatementDoesNotExist() {
		String statement;
		String statementName = "ArraySchema";
		statement = "create objectarray schema SomeArrayEventSchema as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, statementName);

		assertFalse(engine.hasStatement("Bielefeld"));
	}

	@Test
	public void testGetStatementNames() {
		String statement;
		String statementName = "ArraySchema";
		statement = "create objectarray schema SomeArrayEventSchema as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, statementName);

		statement = "select count(first_name) as cntFirst_Name from SomeArrayEventSchema";
		engine.addEPLStatement(statement, "ArrayStatement 1");

		statement = "select count(first_name) as cntFirst_Namespace from SomeArrayEventSchema";
		engine.addEPLStatement(statement, "ArrayStatement 2");

		statement = "select count(first_name) as iCanPutAnythingInHere from SomeArrayEventSchema";
		engine.addEPLStatement(statement, "ArrayStatement 3");

		String[] AllNames = engine.getStatementNames();

		assertEquals(6, AllNames.length);

	}

	@Test
	public void testGetStatements() {
		List<EplStatement> statements = engine.getStatements();

		assertEquals(statements.get(1).expression,
				"create map schema SomeMapEventSchema as (first_name string, numbers integer)");
		assertEquals(statements.get(0).expression, "select count(first_name) as cntFirst_Name from SomeMapEventSchema");
	}

	@Test
	public void testgetStatementExpression() {
		String ExpressionToGet = "create map schema SomeMapEventSchema as (first_name string, numbers integer)";
		String ExpressionHasGotten = engine.getStatementExpression("MapSchema");
		assertEquals(ExpressionToGet, ExpressionHasGotten);
	}
}