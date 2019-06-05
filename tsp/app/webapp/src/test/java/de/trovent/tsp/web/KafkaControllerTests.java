package de.trovent.tsp.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
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

import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.ConnectorController;
import com.trovent.streamprocessor.restapi.ConsumerConnector;
import com.trovent.streamprocessor.restapi.ConsumerListEntry;
import com.trovent.streamprocessor.restapi.ProducerConnector;
import com.trovent.streamprocessor.restapi.ProducerListEntry;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
public class KafkaControllerTests {
	
	@LocalServerPort
    private int port;
	
	WebTarget target;

	EplSchema defaultSchema;
	EplSchema schemaTwo;
	EplStatement defaultStatement;
	EplStatement stmtTwo;

	ConnectorController controller;
		
	private String getBaseUri() {
		return "http://localhost:"+port+"/api/";
	}
	
	@BeforeEach
	public void setUp() {
		
		Client c = ClientBuilder.newClient();
		target = c.target(getBaseUri());
		
		defaultSchema = 
				new EplSchema("Cars")
				.add("name", "string")
				.add("power", "integer")
				.add("isCabrio", "boolean");
		defaultStatement = 
				new EplStatement("SportsCarFilter", "select * from Cars where power > 100");

		schemaTwo = 
				new EplSchema("Person")
				.add("name", "string")
				.add("age", "integer");
		stmtTwo = 
				new EplStatement("AdultFilter", "select * from Person where age>=18");
		
		target.path("esper/schema").request().post(Entity.entity(defaultSchema, MediaType.APPLICATION_JSON));
		target.path("esper/statement").request().post(Entity.entity(defaultStatement, MediaType.APPLICATION_JSON));
		
		this.controller = ConnectorController.getInstance();
	}

	@Test
	public void testStatus() {
		Response response = target.path("kafka/status").request().get();
		assertEquals(200, response.getStatus());
	}

	@Test
	public void testPrerequisites() {
		assertNotNull(this.controller);
		Response response = target.path("esper/statement/SportsCarFilter").request().get();
		assertEquals(200, response.getStatus());
	}

	@Test
	public void testGetConsumers() {
		Response response = target.path("kafka/consumers").request().get();
		assertEquals(200, response.getStatus());

		List<ConsumerListEntry> consumers = 
				response.readEntity(new GenericType<ArrayList<ConsumerListEntry>>() {
		});
		int countConsumers = consumers.size();

		TSPEngine.create().addEPLSchema(schemaTwo);
		ConsumerConnector cc = new ConsumerConnector("input", schemaTwo.name);
		int id = controller.connect(cc);

		response = target.path("kafka/consumers").request().get();
		assertEquals(200, response.getStatus());
		consumers = response.readEntity(new GenericType<ArrayList<ConsumerListEntry>>() {
		});
		assertEquals(countConsumers + 1, consumers.size());

		controller.disconnectConsumer(id);
		response = target.path("kafka/consumers").request().get();
		assertEquals(200, response.getStatus());
		consumers = response.readEntity(new GenericType<ArrayList<ConsumerListEntry>>() {
		});
		assertEquals(countConsumers, consumers.size());
	}

	@Test
	public void testGetProducers() {
		Response response = target.path("kafka/producers").request().get();
		assertEquals(200, response.getStatus());

		List<ProducerListEntry> producers = response.readEntity(new GenericType<ArrayList<ProducerListEntry>>() {
		});
		int countProducers = producers.size();

		TSPEngine.create().addEPLSchema(schemaTwo);
		TSPEngine.create().addEPLStatement(stmtTwo);
		ProducerConnector pc = new ProducerConnector("input", stmtTwo.name);
		int id = controller.connect(pc);

		response = target.path("kafka/producers").request().get();
		assertEquals(200, response.getStatus());
		producers = response.readEntity(new GenericType<ArrayList<ProducerListEntry>>() {
		});
		assertEquals(countProducers + 1, producers.size());

		controller.disconnectProducer(id);
		response = target.path("kafka/producers").request().get();
		assertEquals(200, response.getStatus());
		producers = response.readEntity(new GenericType<ArrayList<ProducerListEntry>>() {
		});
		assertEquals(countProducers, producers.size());

	}

	@Test
	public void addConsumer() {
		int countConsumers = controller.getConsumers().size();

		ConsumerConnector cc = new ConsumerConnector("input", defaultSchema.name);
		Response response = target.path("kafka/consumer").request().post(Entity.entity(cc, MediaType.APPLICATION_JSON));

		assertEquals(200, response.getStatus());
		int id = response.readEntity(Integer.class);
		assertNotEquals(0, id);

		assertEquals(countConsumers + 1, controller.getConsumers().size());

		ConsumerConnector cc2 = controller.getConsumers().get(id);
		assertEquals(cc.topic, cc2.topic);
		assertEquals(cc.schemaName, cc2.schemaName);
	}

	@Test
	public void addProducer() {
		int countProducers = controller.getProducers().size();
		ProducerConnector pc = new ProducerConnector("input", defaultStatement.name);
		Response response = target.path("kafka/producer").request().post(Entity.entity(pc, MediaType.APPLICATION_JSON));

		assertEquals(200, response.getStatus());
		int id = response.readEntity(Integer.class);
		assertNotEquals(0, id);

		assertEquals(countProducers + 1, controller.getProducers().size());

		ProducerConnector pc2 = controller.getProducers().get(id);
		assertEquals(pc.topic, pc2.topic);
		assertEquals(pc.eplStatementName, pc2.eplStatementName);
	}

	@Test
	public void deleteConsumer() {
		// delete not existing id
		Response response = target.path("kafka/consumer/0").request().delete();
		assertEquals(400, response.getStatus());

		// create consumer
		int countConsumers = controller.getConsumers().size();
		ConsumerConnector cc = new ConsumerConnector("input", defaultSchema.name);
		int id = controller.connect(cc);
		assertEquals(countConsumers + 1, controller.getConsumers().size());

		// delete existing id
		response = target.path(String.format("kafka/consumer/%d", id)).request().delete();
		assertEquals(204, response.getStatus());
		assertEquals(countConsumers, controller.getConsumers().size());
	}

	@Test
	public void deleteProducer() {
		// delete not existing id
		Response response = target.path("kafka/producer/0").request().delete();
		assertEquals(400, response.getStatus());

		// create producer
		int countProducers = controller.getProducers().size();
		ProducerConnector pc = new ProducerConnector("input", this.defaultStatement.name);
		int id = controller.connect(pc);
		assertEquals(countProducers + 1, controller.getProducers().size());

		// delete existing id
		response = target.path(String.format("kafka/producer/%d", id)).request().delete();
		assertEquals(204, response.getStatus());
		assertEquals(countProducers, controller.getProducers().size());
	}

}
