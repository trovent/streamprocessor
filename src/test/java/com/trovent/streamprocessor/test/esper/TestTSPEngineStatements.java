package com.trovent.streamprocessor.test.esper;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.Test;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.trovent.streamprocessor.esper.TSPEngine;

import junit.framework.TestCase;

public class TestTSPEngineStatements extends TestCase {

	private TSPEngine engine;

	public TestTSPEngineStatements() {
		super();
	}
	
	protected void setUp() throws Exception {
		super.setUp();

		engine = new TSPEngine();
		engine.init();
		
		//creates a 
		String statement;
		statement = "create map schema SomeMapEventSchema as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, "MapSchema");
		
		statement = "select count(first_name) as cntFirst_Name from SomeMapEventSchema";
		engine.addEPLStatement(statement, "MapStatement");
	}

	protected void tearDown() throws Exception {

		engine.shutdown();
		super.tearDown();
	}
	
	@Test
	public void testStartEPLStatement() {
		engine.startEPLStatement("MapStatement");
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapStatement").isStarted());
	}
	
	@Test
	public void testStartEPLStatementSCHEMA() {
		engine.stopEPLStatement("MapSchema");
		assertFalse(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapSchema").isStarted());
		
		engine.startEPLStatement("MapSchema");
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapSchema").isStarted());
	}
	
	@Test
	public void testStartEPLStatementForNonexistantStatement() {
		assertThrows(EPException.class, 
				()-> engine.startEPLStatement("Bielefeld"));
	}
	
	@Test
	public void testStopEPLStatement() {
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapStatement").isStarted());
		engine.stopEPLStatement("MapStatement");
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapStatement").isStopped());
	}
	
	@Test
	public void testStopEPLStatementSCHEMA() {
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapSchema").isStarted());
		engine.stopEPLStatement("MapSchema");
		assertTrue(engine.getEPServiceProvider().getEPAdministrator().getStatement("MapSchema").isStopped());
	}
	
	@Test
	public void testStopEPLStatementForNonexistantStatement() {
		assertThrows(EPException.class, 
				()-> engine.stopEPLStatement("Bielefeld"));		
	}
	
	@Test//TODO
	public void testRemoveEPLStatement() {
		engine.removeEPLStatement("MapStatement");
	}
	
	@Test//TODO
	public void testRemoveEPLStatementSCHEMA() {
		engine.removeEPLStatement("MapSchema");
	}
	
	@Test
	public void testRemoveEPLStatementForNonexistantStatement() {
		assertThrows(EPException.class, 
				()-> engine.removeEPLStatement("Bielefeld"));
	}
	
	@Test
	public void testAddListenerToStatement() throws InterruptedException {
		class MyListener implements UpdateListener {
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				//does Nothing
			}
		}
		MyListener newListener = new MyListener();
		engine.addListener("MapStatement", newListener);
	}
	
	@Test
	public void testAddListenerToNonexistantStatement() {
		class MyListener implements UpdateListener {			
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				//does Nothing
			}
		}
		assertThrows(EPException.class, 
				()-> engine.addListener("Bielefeld", new MyListener()));		
	}
	
}