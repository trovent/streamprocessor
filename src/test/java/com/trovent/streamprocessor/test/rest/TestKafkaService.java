package com.trovent.streamprocessor.test.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.kafka.ConnectorController;
import com.trovent.streamprocessor.restapi.ApplicationServer;
import com.trovent.streamprocessor.restapi.ConsumerConnector;
import com.trovent.streamprocessor.restapi.ProducerConnector;

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

	@Test
	void testGetConsumers() {
		Response response = target.path("api/consumers").request().get();
		assertEquals(200, response.getStatus());
	}

	@Test
	void addConsumer() {
		ConsumerConnector cc = new ConsumerConnector("input", defaultSchema.name);
		Response response = target.path("api/consumer").request().post(Entity.entity(cc, MediaType.APPLICATION_JSON));

		assertEquals(200, response.getStatus());
		int id = response.readEntity(Integer.class);
		assertNotEquals(0, id);

		ConnectorController controller = ConnectorController.getInstance();
		assertEquals(1, controller.getConsumers().size());

		ConsumerConnector cc2 = controller.getConsumers().get(id);
		assertEquals(cc.topic, cc2.topic);
		assertEquals(cc.schemaName, cc2.schemaName);
	}

	@Test
	void addProducer() {
		ProducerConnector pc = new ProducerConnector("input", defaultStatement.name);
		Response response = target.path("api/producer").request().post(Entity.entity(pc, MediaType.APPLICATION_JSON));

		assertEquals(200, response.getStatus());
		int id = response.readEntity(Integer.class);
		assertNotEquals(0, id);

		ConnectorController controller = ConnectorController.getInstance();
		assertEquals(1, controller.getProducers().size());

		ProducerConnector pc2 = controller.getProducers().get(id);
		assertEquals(pc.topic, pc2.topic);
		assertEquals(pc.eplStatementName, pc2.eplStatementName);
	}

	@Test
	void deleteConsumer() {
		Response response = target.path("api/consumer/666").request().delete();
		fail("not implemented");
	}

	@Test
	void deleteProducer() {
		Response response = target.path("api/producer/667").request().delete();
		fail("not implemented");
	}
}
