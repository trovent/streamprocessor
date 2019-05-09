package com.trovent.streamprocessor.test.rest;

import static org.junit.jupiter.api.Assertions.fail;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.restapi.ApplicationServer;
import com.trovent.streamprocessor.restapi.ConsumerConnector;

public class TestKafkaService {
	ApplicationServer server;
	WebTarget target;

	EplSchema defaultSchema;
	EplStatement defaultStatement;

	@BeforeEach
	protected void setUp() {
		defaultSchema = new EplSchema("Cars").add("name", "string").add("power", "integer").add("isCabrio", "boolean");
		defaultStatement = new EplStatement("SportsCarFilter", "select * from Cars where power > 100");

		server = new ApplicationServer(null);
		server.start();

		Client c = ClientBuilder.newClient();
		target = c.target(ApplicationServer.BASE_URI);

		target.path("api/schema").request().post(Entity.entity(defaultSchema, MediaType.APPLICATION_JSON));
		target.path("api/statement").request().post(Entity.entity(defaultStatement, MediaType.APPLICATION_JSON));
	}

	@AfterEach
	protected void tearDown() {
		server.stop();
	}

	@Test
	void testPrerequisites() {
		Response response = target.path("api/statement/SportsCarFilter").request().get();
		assertEquals(200, response.getStatus());
	}

	@Test
	void testConsumerConnector() {
		String topic = "input";
		String schemaName = defaultSchema.name;
		ConsumerConnector connector = new ConsumerConnector(topic, schemaName);

	}
}
