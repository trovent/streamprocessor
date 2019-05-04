package com.trovent.streamprocessor.test.rest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Test;

import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.restapi.ApplicationServer;

import junit.framework.TestCase;

public class TestEsperService extends TestCase {

	ApplicationServer server;
	WebTarget target;

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
}
