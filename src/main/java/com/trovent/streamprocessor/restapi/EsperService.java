package com.trovent.streamprocessor.restapi;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.trovent.streamprocessor.esper.EplStatement;

@Path("api")
public class EsperService {

	@GET
	@Path("hello")
	@Produces(MediaType.TEXT_PLAIN)
	public String hello() {
		return "Hello World!\n";
	}

	@GET
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
}
