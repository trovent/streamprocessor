package com.trovent.streamprocessor.test.rest;

import java.util.LinkedHashMap;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Test;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.restapi.ApplicationServer;
import com.trovent.streamprocessor.restapi.EsperService;

import junit.framework.TestCase;

public class TestEsperService extends TestCase {

	ApplicationServer server;
	WebTarget target;

	EplStatement stmtTwo;

	protected void setUp() {
		server = new ApplicationServer(null);
		server.start();

		Client c = ClientBuilder.newClient();

		target = c.target(ApplicationServer.BASE_URI);

		// create schema "one" and add it to engine for testing
		EplSchema schema = new EplSchema("one").add("name", "string").add("age", "integer").add("isAdult", "boolean");
		target.path("api/schema").request().post(Entity.entity(schema, MediaType.APPLICATION_JSON));

		// create schema "two" and add it to engine for testing
		schema = new EplSchema("two").add("name", "string").add("age", "integer");
		target.path("api/schema").request().post(Entity.entity(schema, MediaType.APPLICATION_JSON));

		stmtTwo = new EplStatement("stmtTwo", "select * from one");
		target.path("api/statement").request().post(Entity.entity(stmtTwo, MediaType.APPLICATION_JSON));

		EplStatement stmtThree = new EplStatement("stmtThree", "select name from one");
		target.path("api/statement").request().post(Entity.entity(stmtThree, MediaType.APPLICATION_JSON));
	}

	protected void tearDown() {
		server.stop();
	}

	@Test
	public void testAddSchema() {
		EplSchema schema = new EplSchema("zero");
		schema.fields.put("name", "string");
		schema.fields.put("age", "integer");
		schema.fields.put("isAdult", "boolean");

		Response response = target.path("api/schema").request().post(Entity.entity(schema, MediaType.APPLICATION_JSON));
		String result = response.readEntity(String.class);
		assertEquals(schema.name, result);
	}

	@Test
	public void testGetSchemaFailed() {
		Response response = target.path("api/schema/notexisting").request().get();
		EplSchema schema = response.readEntity(EplSchema.class);
		assertEquals(404, response.getStatus());
		assertNull(schema);
	}

	@Test
	public void testGetSchemaSuccess() {
		Response response = target.path("api/schema/one").request().get();
		EplSchema schema = response.readEntity(EplSchema.class);
		assertEquals(200, response.getStatus());
		assertEquals("one", schema.name);
		assertTrue(schema.fields.containsKey("name"));
		assertTrue(schema.fields.containsKey("age"));
		assertTrue(schema.fields.containsKey("isAdult"));
	}

	@Test
	public void testDeleteSchemaSuccess() {
		Response response = target.path("api/schema/two").request().delete();
		assertEquals(response.readEntity(String.class), 200, response.getStatus());
	}

	@Test
	public void testDeleteSchemaFailed() {
		Response response = target.path("api/schema/notexisting").request().delete();
		assertEquals(404, response.getStatus());
	}

	@Test
	public void testGetSchemasSuccess() {
		Response response = target.path("api/schemas/").request().get();
		assertEquals(200, response.getStatus());

		List<EplSchema> result = response.readEntity(new GenericType<List<EplSchema>>() {
		});
		assertTrue(result.size() > 0);
	}

	@Test
	public void testAddStatementFailed() {
		EplStatement statement = new EplStatement("zeroStmt");
		statement.expression = "select * from notexisting";

		Response response = target.path("api/statement").request()
				.post(Entity.entity(statement, MediaType.APPLICATION_JSON));

		assertEquals(412, response.getStatus());
	}

	@Test
	public void testAddStatementSuccess() {
		EplStatement statement = new EplStatement("oneStmt");
		statement.expression = "select * from one";

		Response response = target.path("api/statement").request()
				.post(Entity.entity(statement, MediaType.APPLICATION_JSON));
		assertEquals(200, response.getStatus());

		String result = response.readEntity(String.class);
		assertEquals(statement.name, result);
	}

	@Test
	public void testGetStatementFailed() {
		Response response = target.path("api/statement/notexisting").request().get();

		assertEquals(404, response.getStatus());
	}

	@Test
	public void testGetStatementSuccess() {
		Response response = target.path("api/statement/stmtTwo").request().get();
		assertEquals(200, response.getStatus());

		EplStatement result = response.readEntity(EplStatement.class);
		assertEquals(this.stmtTwo.name, result.name);
		assertEquals(this.stmtTwo.expression, result.expression);
	}

	@Test
	public void testGetStatementsSuccess() {
		Response response = target.path("api/statements/").request().get();
		assertEquals(200, response.getStatus());

		List<EplStatement> result = response.readEntity(new GenericType<List<EplStatement>>() {
		});
		assertTrue(result.size() > 0);
	}

	@Test
	public void testDeleteStatementSuccess() {
		Response response = target.path("api/statement/stmtThree").request().delete();
		assertEquals(200, response.getStatus());
	}

	@Test
	public void testDeleteStatementFailed() {
		Response response = target.path("api/statement/unknown").request().delete();
		assertEquals(404, response.getStatus());
	}

	@Test
	public void testSendEvent() {
		String STATEMENTNAME = "counter";
		EplStatement countingStatement = new EplStatement(STATEMENTNAME,
				"select count(name) as Number_of_Names from one");
		Response response = target.path("api/statement").request()
				.post(Entity.entity(countingStatement, MediaType.APPLICATION_JSON));
		assertEquals(200, response.getStatus());

		LinkedHashMap<String, Object> myMap = new LinkedHashMap<>();
		myMap.put("name", "Leo");
		myMap.put("age", 23);
		myMap.put("isAdult", false);

		EplEvent myEvent = new EplEvent();
		myEvent.eventTypeName = "one";
		myEvent.data = myMap;

		class ConsoleListener implements UpdateListener {

			public int count = 0;

			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				long namen = (long) newEvents[0].get("Number_of_Names");
				System.out.println(String.format("NumberOfNames: %d", namen));
				count++;
			}
		}

		ConsoleListener myListener = new ConsoleListener();
		EsperService.getEngine().addListener(STATEMENTNAME, myListener);

		// this works
		// EsperService.getEngine().sendEPLEvent(myEvent.eventTypeName, myEvent.data);

		response = target.path("api/sendEvent").request().post(Entity.entity(myEvent, MediaType.APPLICATION_JSON));

		assertEquals(200, response.getStatus());

		while (myListener.count == 00)
			;

	}
}
