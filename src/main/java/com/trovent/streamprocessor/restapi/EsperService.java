package com.trovent.streamprocessor.restapi;

import java.util.function.Consumer;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.internal.util.Producer;

import com.trovent.streamprocessor.esper.EplStatement;

@Path("api")
public class EsperService {

	@GET
	@Path("hello")
	@Produces(MediaType.TEXT_PLAIN)
	public String hello() {
		return "Hello World!\n";
	}

	@POST
	@Path("addEplStatement")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public EplStatement addEplStatement(EplStatement stmt) {
		// TODO create statement, add to engine, add to list, return id
		return stmt;
	}

	public void deleteEplStatement(String name) {
		// TODO: delete from engine and list
		return;
	}

	// { "topic" : "mytopic", "eventname" : "myeventname" }
	@POST
	@Path("addConsumer")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public int addConsumer(Consumer consumer) {
		int id = 0;

		return id;
	}

	// { "topic" : "mytopic", "statementname" : "mystmtname" }
	@POST
	@Path("addProducer")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public int addProducer(Producer producer) {
		int id = 0;

		return id;
	}

	@DELETE
	@Path("consumer/{id}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void deleteConsumer(@QueryParam("id") int id) {

	}

	@DELETE
	@Path("producer/{id}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void deleteProducer(@QueryParam("id") int id) {

	}

	@POST
	@Path("sendEvent")
	@Consumes(MediaType.APPLICATION_JSON)
	public void sendEvent(GenericEventType event) {
		// event.data["fieldname"] = "value"
	}

}
