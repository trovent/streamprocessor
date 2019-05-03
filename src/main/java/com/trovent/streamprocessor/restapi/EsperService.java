package com.trovent.streamprocessor.restapi;

import java.util.function.Consumer;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.internal.util.Producer;

import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;

@Path("api")
public class EsperService {

	static TSPEngine epService = null;

	public EsperService() {
		if (epService == null)
			epService = TSPEngine.create();
	}

	@GET
	@Path("statement/{name}")
	@Produces(MediaType.TEXT_PLAIN)
	public EplStatement getEplStatement(@PathParam("name") String name) {
		EplStatement stmt = new EplStatement();
		if (epService.hasStatement(name)) {
			stmt.name = name;
			stmt.expression = epService.getStatementExpression(name);
			return stmt;
		}
		return stmt;
	}

	@POST
	@Path("statement")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public String addEplStatement(EplStatement stmt) {
		return epService.addEPLStatement(stmt.name, stmt.expression);
	}

	@DELETE
	@Path("statement/{name}")
	public void deleteEplStatement(@PathParam("name") String name) {
		epService.removeEPLStatement(name);
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
