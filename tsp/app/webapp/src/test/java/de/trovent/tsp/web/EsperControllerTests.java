package de.trovent.tsp.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.kafka.ProducerListener;
import com.trovent.streamprocessor.kafka.StringQueueProducer;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
public class EsperControllerTests {
	
	@LocalServerPort
    private int port;
	
	WebTarget target;
	
	EplStatement stmtTwo;
	
	private String getBaseUri() {
		return "http://localhost:"+port+"/api/";
	}
	
	@BeforeEach
	public void setUp() {
		
		Client c = ClientBuilder.newClient();
		target = c.target(getBaseUri());

		// create schema "one" and add it to engine for testing
		EplSchema schema = new EplSchema("one").add("name", "string").add("age", "integer").add("isAdult", "boolean");
		target.path("esper/schema").request().post(Entity.entity(schema, MediaType.APPLICATION_JSON));

		// create schema "two" and add it to engine for testing
		schema = new EplSchema("two").add("name", "string").add("age", "integer");
		target.path("esper/schema").request().post(Entity.entity(schema, MediaType.APPLICATION_JSON));

		stmtTwo = new EplStatement("stmtTwo", "select * from one");
		target.path("esper/statement").request().post(Entity.entity(stmtTwo, MediaType.APPLICATION_JSON));

		EplStatement stmtThree = new EplStatement("stmtThree", "select name from one");
		target.path("esper/statement").request().post(Entity.entity(stmtThree, MediaType.APPLICATION_JSON));
	}
	
	@Test
	public void testStatus() {
		Response response = target.path("esper/status").request().get();
		assertEquals(200, response.getStatus());
	}
	
	@Test
	public void testAddSchema() {
		EplSchema schema = new EplSchema("zero");
		schema.add("name", "string").add("age", "integer").add("isAdult", "boolean");

		Response response = target.path("esper/schema").request().post(Entity.entity(schema, MediaType.APPLICATION_JSON));
		assertEquals(201, response.getStatus());
	}

	@Test
	public void testGetSchemaFailed() {
		Response response = target.path("esper/schema/notexisting").request().get();
		EplSchema schema = response.readEntity(EplSchema.class);
		assertNull(schema);
	}

	@Test
	public void testGetSchemaSuccess() {
		Response response = target.path("esper/schema/one").request().get();
		EplSchema schema = response.readEntity(EplSchema.class);
		assertEquals(200, response.getStatus());
		assertEquals("one", schema.name);
		assertTrue(schema.fields.containsKey("name"));
		assertTrue(schema.fields.containsKey("age"));
		assertTrue(schema.fields.containsKey("isAdult"));
	}

	@Test
	public void testDeleteSchemaSuccess() {
		Response response = target.path("esper/schema/two").request().delete();
		assertEquals(204, response.getStatus(), response.readEntity(String.class));
	}

	@Test
	public void testDeleteSchemaFailed() {
		Response response = target.path("esper/schema/notexisting").request().delete();
		assertEquals(400, response.getStatus());
	}

	@Test
	public void testGetSchemasSuccess() {
		Response response = target.path("esper/schemas").request().get();
		assertEquals(200, response.getStatus());

		List<EplSchema> result = response.readEntity(new GenericType<List<EplSchema>>() {
		});
		assertTrue(result.size() > 0);
	}

	@Test
	public void testAddStatementFailed() {
		EplStatement statement = new EplStatement("zeroStmt");
		statement.expression = "select * from notexisting";

		Response response = target.path("esper/statement").request()
				.post(Entity.entity(statement, MediaType.APPLICATION_JSON));

		assertEquals(400, response.getStatus());
	}

	@Test
	public void testAddStatementSuccess() {
		EplStatement statement = new EplStatement("oneStmt");
		statement.expression = "select * from one";

		Response response = target.path("esper/statement").request()
				.post(Entity.entity(statement, MediaType.APPLICATION_JSON));
		assertEquals(201, response.getStatus());
	}

	@Test
	public void testGetStatementFailed() {
		Response response = target.path("esper/statement/notexisting").request().get();
		EplStatement result = response.readEntity(EplStatement.class);
		assertNull(result);
	}

	@Test
	public void testGetStatementSuccess() {
		Response response = target.path("esper/statement/stmtTwo").request().get();
		assertEquals(200, response.getStatus());

		EplStatement result = response.readEntity(EplStatement.class);
		assertEquals(this.stmtTwo.name, result.name);
		assertEquals(this.stmtTwo.expression, result.expression);
	}

	@Test
	public void testGetStatementsSuccess() {
		Response response = target.path("esper/statements/").request().get();
		assertEquals(200, response.getStatus());

		List<EplStatement> result = response.readEntity(new GenericType<List<EplStatement>>() {
		});
		assertTrue(result.size() > 0);
	}

	@Test
	public void testDeleteStatementSuccess() {
		Response response = target.path("esper/statement/stmtThree").request().delete();
		assertEquals(204, response.getStatus());
	}

	@Test
	public void testDeleteStatementFailed() {
		Response response = target.path("esper/statement/unknown").request().delete();
		assertEquals(400, response.getStatus());
	}

	@Test
	public void testSendEvent() throws InterruptedException {
		String STATEMENTNAME = "counter";
		EplStatement countingStatement = new EplStatement(STATEMENTNAME,
				"select count(name) as Number_of_Names from one");
		Response response = target.path("esper/statement").request()
				.post(Entity.entity(countingStatement, MediaType.APPLICATION_JSON));
		assertEquals(201, response.getStatus());

		StringQueueProducer producer = new StringQueueProducer();
		EsperController.getEngine().addListener(STATEMENTNAME, new ProducerListener(producer));

		EplEvent myEvent = new EplEvent("one").add("name", "Leo").add("age", 23).add("isAdult", false);
		response = target.path("esper/sendEvent/map").request().post(Entity.entity(myEvent, MediaType.APPLICATION_JSON));

		assertEquals(200, response.getStatus());

		while (producer.isEmpty()) {
			Thread.sleep(10);
		}

		assertEquals(1, producer.count());
	}

}
