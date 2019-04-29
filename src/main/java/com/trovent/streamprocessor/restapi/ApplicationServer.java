package com.trovent.streamprocessor.restapi;

import java.net.URI;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import com.trovent.streamprocessor.Configuration;

public class ApplicationServer {
	
	public static final String BASE_URI = "http://localhost:8080/";
	
	private HttpServer httpServer;
	
	Configuration config;
	
	public ApplicationServer(Configuration config)	{
		this.config = config;
	}
	
	public void start() {
		final ResourceConfig rc = new ResourceConfig().packages("com.trovent.streamprocessor.restapi");

        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        this.httpServer = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
	}
	
	public void stop()	{
		if (this.httpServer!=null)
			this.httpServer.shutdownNow();
	}
}
