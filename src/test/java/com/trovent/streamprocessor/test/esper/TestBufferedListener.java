package com.trovent.streamprocessor.test.esper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.trovent.streamprocessor.esper.BufferedListener;
import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;

class TestBufferedListener {

	TSPEngine engine = TSPEngine.create();
	private EplSchema schema;
	private EplStatement statement;
	private BufferedListener listener;
	private EplEvent eventAlice;
	private EplEvent eventBob;
	private EplEvent eventEve;

	@BeforeEach
	void setUp() throws Exception {

		engine.init();
		schema = new EplSchema("People").add("name", "string").add("age", "integer");
		statement = new EplStatement("Adult", "select * from People where age>=18");

		engine.addEPLSchema(schema);
		engine.addEPLStatement(statement);

		listener = new BufferedListener();
		engine.addListener(statement.name, listener);

		eventAlice = new EplEvent(schema.name).add("name", "Alice").add("age", 20);
		eventBob = new EplEvent(schema.name).add("name", "Bob").add("age", 5);
		eventEve = new EplEvent(schema.name).add("name", "Eve").add("age", 42);
	}

	@AfterEach
	void tearDown() throws Exception {
		engine.shutdown();
	}

	@Test
	void testPoll() {
		assertNull(listener.poll());
		engine.sendEPLEvent(eventAlice);
		engine.sendEPLEvent(eventBob);
		engine.sendEPLEvent(eventEve);

		EplEvent event = listener.poll();
		assertEquals(eventAlice.data.get("name"), event.data.get("name"));
		event = listener.poll();
		assertEquals(eventEve.data.get("name"), event.data.get("name"));
	}

	@Test
	void testPeek() {
		assertNull(listener.peek());
		engine.sendEPLEvent(eventAlice);

		assertEquals(1, listener.size());
		assertEquals(eventAlice.data.get("name"), listener.peek().data.get("name"));
		assertEquals(1, listener.size());
		assertEquals(eventAlice.data.get("name"), listener.peek().data.get("name"));
		assertEquals(1, listener.size());
	}

	@Test
	void testSize() {
		assertEquals(0, listener.size());

		engine.sendEPLEvent(eventAlice);
		assertEquals(1, listener.size());

		engine.sendEPLEvent(eventBob);
		assertEquals(1, listener.size());

		engine.sendEPLEvent(eventEve);
		assertEquals(2, listener.size());

		listener.poll();
		assertEquals(1, listener.size());

		listener.poll();
		assertEquals(0, listener.size());
	}

	@Test
	void testIsEmpty() {
		assertTrue(listener.isEmpty());

		engine.sendEPLEvent(eventAlice);
		assertFalse(listener.isEmpty());

		listener.poll();
		assertTrue(listener.isEmpty());

		engine.sendEPLEvent(eventBob);
		assertTrue(listener.isEmpty());
	}

}
