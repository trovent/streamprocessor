package com.trovent.streamprocessor.restapi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import javax.ws.rs.core.Response;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.espertech.esper.client.EventType;
import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;

@Path("api")
public class EsperService {

	static TSPEngine epService = null;

	public EsperService() {
		if (epService == null) {
			epService = TSPEngine.create();
			epService.init();
		}
	}

	@GET
	@Path("statement/{name}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getEplStatement(@PathParam("name") String name) {
		EplStatement stmt = new EplStatement();
		try {
			if (epService.hasStatement(name)) {
				stmt.name = name;
				stmt.expression = epService.getStatementExpression(name);
				return Response.status(200).entity(stmt).build();
			} else {
				return Response.status(404).build();
			}
		} catch (EPException e) {
			return Response.status(412).entity(e.toString()).build();
		}
	}

	@POST
	@Path("statement")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public Response addEplStatement(EplStatement stmt) {
		try {
			String name = epService.addEPLStatement(stmt.expression, stmt.name);
			return Response.status(200).entity(name).build();
		} catch (EPException e) {
			return Response.status(412).entity(e.toString()).build();
		}
	}

	@DELETE
	@Path("statement/{name}")
	public Response deleteEplStatement(@PathParam("name") String name) {
		try {

			if (epService.hasStatement(name)) {
				epService.removeEPLStatement(name);
				return Response.status(200).build();
			}
			return Response.status(404).build();
		} catch (EPException e) {
			return Response.status(404).entity(e.toString()).build();
		}
	}

	@GET
	@Path("statements")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getEplStatements() {
		Map<String, String> statements = epService.getStatements();
		List<EplStatement> eplStatements = new ArrayList<EplStatement>();
		for (Map.Entry<String, String> statement : statements.entrySet()) {
			eplStatements.add(new EplStatement(statement.getKey(), statement.getValue()));
		}
		return Response.status(200).entity(eplStatements).build();
	}

	@GET
	@Path("schema/{name}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getEplSchema(@PathParam("name") String name) {
		EplSchema schema = new EplSchema();
		if (epService.hasEPLSchema(name)) {
			schema.name = name;
			EventType eventType = epService.getEPLSchema(name);
			for (EventPropertyDescriptor prop : eventType.getPropertyDescriptors()) {
				schema.fields.put(prop.getPropertyName(), prop.getPropertyType().getName());
			}
			return Response.status(200).entity(schema).build();
		}
		return Response.status(404).build();
	}

	@POST
	@Path("schema")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public String addEplSchema(EplSchema schema) {
		epService.addEPLSchema(schema.name, schema.fields);
		return schema.name;
	}

	@DELETE
	@Path("schema/{name}")
	public Response deleteEplSchema(@PathParam("name") String name) {
		if (epService.hasEPLSchema(name)) {
			try {
				epService.removeEPLSchema(name);
				return Response.status(200).build();
			} catch (EPException e) {
				return Response.status(428).entity(e.toString()).build();
			}
		}
		return Response.status(404).build();
	}

	@GET
	@Path("schemas")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getEplSchemas() {
		EventType[] eventTypes = epService.getEPLSchemas();
		List<EplSchema> schemas = new ArrayList<EplSchema>();
		for (EventType eventType : eventTypes) {
			EplSchema schema = new EplSchema(eventType.getName());
			for (EventPropertyDescriptor propDescriptor : eventType.getPropertyDescriptors()) {
				schema.add(propDescriptor.getPropertyName(), propDescriptor.getPropertyType().toString());
			}
			schemas.add(schema);
		}
		return Response.status(200).entity(schemas).build();
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
