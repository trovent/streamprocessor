package com.trovent.streamprocessor.restapi;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("api")
public class KafkaService {

	public KafkaService() {

	}

	// [ { "id" : <id>,
	// { "topic" : "mytopic", "schema" : "myeventname" }
	// },
	// ... ]
	@GET
	@Path("consumers")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getConsumers() {

		return Response.status(404).build();
	}

	@GET
	@Path("producers")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getProducers() {

		return Response.status(404).build();
	}

	// { "topic" : "mytopic", "schema" : "myeventname" }
	// return: id
	@POST
	@Path("consumer")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public Response addConsumer(Connector connector) {

		return Response.status(404).build();
	}

	// { "topic" : "mytopic", "statement" : "mystatementname" }
	// return: id
	@POST
	@Path("producer")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public Response addProducer(Connector connector) {

		return Response.status(404).build();
	}

	@DELETE
	@Path("consumer/{id}")
	public Response deleteConsumer(@QueryParam("id") int id) {

		return Response.status(404).build();
	}

	@DELETE
	@Path("producer/{id}")
	public Response deleteProducer(@QueryParam("id") int id) {

		return Response.status(404).build();
	}

}
