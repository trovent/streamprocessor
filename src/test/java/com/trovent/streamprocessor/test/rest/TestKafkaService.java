package com.trovent.streamprocessor.test.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.ConnectorController;
import com.trovent.streamprocessor.restapi.ApplicationServer;
import com.trovent.streamprocessor.restapi.ConsumerConnector;
import com.trovent.streamprocessor.restapi.ConsumerListEntry;
import com.trovent.streamprocessor.restapi.KafkaService;
import com.trovent.streamprocessor.restapi.ProducerConnector;
import com.trovent.streamprocessor.restapi.ProducerListEntry;

public class TestKafkaService {
	ApplicationServer server;
	WebTarget target;

	EplSchema defaultSchema;
	EplSchema schemaTwo;
	EplStatement defaultStatement;
	EplStatement stmtTwo;

	ConnectorController controller;

	@BeforeEach
	protected void setUp() {
		defaultSchema = new EplSchema("Cars").add("name", "string").add("power", "integer").add("isCabrio", "boolean");
		defaultStatement = new EplStatement("SportsCarFilter", "select * from Cars where power > 100");

		schemaTwo = new EplSchema("Person").add("name", "string").add("age", "integer");
		stmtTwo = new EplStatement("AdultFilter", "select * from Person where age>=18");

		server = new ApplicationServer(null);
		server.start();

		Client c = ClientBuilder.newClient();
		target = c.target(ApplicationServer.BASE_URI);

		target.path("api/schema").request().post(Entity.entity(defaultSchema, MediaType.APPLICATION_JSON));
		target.path("api/statement").request().post(Entity.entity(defaultStatement, MediaType.APPLICATION_JSON));

		// run Constructor of the service to have a Controller instance created
		new KafkaService();
		// get the controller (for debugging/testing)
		controller = ConnectorController.getInstance();
	}

	@AfterEach
	protected void tearDown() {
		server.stop();
	}

	@Test
	void testPrerequisites() {
		assertNotNull(this.controller);
		Response response = target.path("api/statement/SportsCarFilter").request().get();
		assertEquals(200, response.getStatus());
	}

	@Test
	void testGetConsumers() {
		Response response = target.path("api/consumers").request().get();
		assertEquals(200, response.getStatus());

		ArrayList<ConsumerListEntry> consumers = response.readEntity(new GenericType<ArrayList<ConsumerListEntry>>() {
		});
		int countConsumers = consumers.size();

		TSPEngine.create().addEPLSchema(schemaTwo);
		ConsumerConnector cc = new ConsumerConnector("input", schemaTwo.name);
		int id = controller.connect(cc);

		response = target.path("api/consumers").request().get();
		assertEquals(200, response.getStatus());
		consumers = response.readEntity(new GenericType<ArrayList<ConsumerListEntry>>() {
		});
		assertEquals(countConsumers + 1, consumers.size());

		controller.disconnectConsumer(id);
		response = target.path("api/consumers").request().get();
		assertEquals(200, response.getStatus());
		consumers = response.readEntity(new GenericType<ArrayList<ConsumerListEntry>>() {
		});
		assertEquals(countConsumers, consumers.size());
	}

	@Test
	void testGetProducers() {
		Response response = target.path("api/producers").request().get();
		assertEquals(200, response.getStatus());

		ArrayList<ProducerListEntry> producers = response.readEntity(new GenericType<ArrayList<ProducerListEntry>>() {
		});
		int countProducers = producers.size();

		TSPEngine.create().addEPLSchema(schemaTwo);
		TSPEngine.create().addEPLStatement(stmtTwo);
		ProducerConnector pc = new ProducerConnector("input", stmtTwo.name);
		int id = controller.connect(pc);

		response = target.path("api/producers").request().get();
		assertEquals(200, response.getStatus());
		producers = response.readEntity(new GenericType<ArrayList<ProducerListEntry>>() {
		});
		assertEquals(countProducers + 1, producers.size());

		controller.disconnectProducer(id);
		response = target.path("api/producers").request().get();
		assertEquals(200, response.getStatus());
		producers = response.readEntity(new GenericType<ArrayList<ProducerListEntry>>() {
		});
		assertEquals(countProducers, producers.size());

	}

	@Test
	void addConsumer() {
		int countConsumers = controller.getConsumers().size();

		ConsumerConnector cc = new ConsumerConnector("input", defaultSchema.name);
		Response response = target.path("api/consumer").request().post(Entity.entity(cc, MediaType.APPLICATION_JSON));

		assertEquals(200, response.getStatus());
		int id = response.readEntity(Integer.class);
		assertNotEquals(0, id);

		assertEquals(countConsumers + 1, controller.getConsumers().size());

		ConsumerConnector cc2 = controller.getConsumers().get(id);
		assertEquals(cc.topic, cc2.topic);
		assertEquals(cc.schemaName, cc2.schemaName);
	}

	@Test
	void addProducer() {
		int countProducers = controller.getProducers().size();
		ProducerConnector pc = new ProducerConnector("input", defaultStatement.name);
		Response response = target.path("api/producer").request().post(Entity.entity(pc, MediaType.APPLICATION_JSON));

		assertEquals(200, response.getStatus());
		int id = response.readEntity(Integer.class);
		assertNotEquals(0, id);

		assertEquals(countProducers + 1, controller.getProducers().size());

		ProducerConnector pc2 = controller.getProducers().get(id);
		assertEquals(pc.topic, pc2.topic);
		assertEquals(pc.eplStatementName, pc2.eplStatementName);
	}

	@Test
	void deleteConsumer() {
		// delete not existing id
		Response response = target.path("api/consumer/666").request().delete();
		assertEquals(404, response.getStatus());

		// create consumer
		int countConsumers = controller.getConsumers().size();
		ConsumerConnector cc = new ConsumerConnector("input", defaultSchema.name);
		int id = controller.connect(cc);
		assertEquals(countConsumers + 1, controller.getConsumers().size());

		// delete existing id
		response = target.path(String.format("api/consumer/%d", id)).request().delete();
		assertEquals(200, response.getStatus());
		assertEquals(countConsumers, controller.getConsumers().size());
	}

	@Test
	void deleteProducer() {
		// delete not existing id
		Response response = target.path("api/producer/666").request().delete();
		assertEquals(404, response.getStatus());

		// create producer
		int countProducers = controller.getProducers().size();
		ProducerConnector pc = new ProducerConnector("input", this.defaultStatement.name);
		int id = controller.connect(pc);
		assertEquals(countProducers + 1, controller.getProducers().size());

		// delete existing id
		response = target.path(String.format("api/producer/%d", id)).request().delete();
		assertEquals(200, response.getStatus());
		assertEquals(countProducers, controller.getProducers().size());
	}
}
