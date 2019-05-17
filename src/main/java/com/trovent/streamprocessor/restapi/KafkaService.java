package com.trovent.streamprocessor.restapi;

import java.util.ArrayList;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.ConnectorController;
import com.trovent.streamprocessor.kafka.KafkaManager;

@Path("api")
public class KafkaService {

	static ConnectorController connectorController;

	public KafkaService() {
		if (connectorController == null) {
			TSPEngine engine = TSPEngine.create();
			KafkaManager kafkaManager = new KafkaManager();
			connectorController = ConnectorController.create(engine, kafkaManager);
		}
	}

	/*
	 * [ { "id" : <id>, "connector" : { "topic" : "mytopic", "schema" :
	 * "myeventname" } }, ... ]
	 * 
	 * return: List<ConsumerListEntry>
	 */
	@GET
	@Path("consumers")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getConsumers() {

		Map<Integer, ConsumerConnector> connectors = connectorController.getConsumers();

		// Transform into JSON compatible format
		ArrayList<ConsumerListEntry> consumerList = new ArrayList<ConsumerListEntry>();
		connectors.forEach((id, connector) -> consumerList.add(new ConsumerListEntry(id, connector)));

		Response response = Response.status(200).entity(consumerList).build();

		return response;
	}

	// return: List<ProducerListEntry>
	@GET
	@Path("producers")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getProducers() {

		Map<Integer, ProducerConnector> producers = connectorController.getProducers();

		// Transform into JSON compatible format
		ArrayList<ProducerListEntry> producerList = new ArrayList<>();
		producers.forEach((id, connector) -> producerList.add(new ProducerListEntry(id, connector)));

		Response response = Response.status(200).entity(producerList).build();

		return response;
	}

	// { "topic" : "mytopic", "schemaName" : "myeventname" }
	// return: id
	@POST
	@Path("consumer")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public Response addConsumer(ConsumerConnector connector) {

		int hashCode = connectorController.connect(connector);
		return Response.status(200).entity(hashCode).build();
	}

	// { "topic" : "mytopic", "eplStatementName" : "mystatementname" }
	// return: id
	@POST
	@Path("producer")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public Response addProducer(ProducerConnector connector) {

		int hashCode = connectorController.connect(connector);
		return Response.status(200).entity(hashCode).build();
	}

	@DELETE
	@Path("consumer/{id}")
	public Response deleteConsumer(@PathParam("id") int id) {

		if (connectorController.disconnectConsumer(id)) {
			return Response.status(200).build();
		}
		return Response.status(404).build();
	}

	@DELETE
	@Path("producer/{id}")
	public Response deleteProducer(@PathParam("id") int id) {

		if (connectorController.disconnectProducer(id)) {
			return Response.status(200).build();
		}
		return Response.status(404).build();
	}

}
