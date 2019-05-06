package com.trovent.streamprocessor.test.rest;

import javax.ws.rs.client.WebTarget;

import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.restapi.ApplicationServer;

import junit.framework.TestCase;

public class TestKafkaService extends TestCase {
	ApplicationServer server;
	WebTarget target;

	EplStatement stmtTwo;

	protected void setUp() {

	}

	protected void tearDown() {
		server.stop();
	}
}
