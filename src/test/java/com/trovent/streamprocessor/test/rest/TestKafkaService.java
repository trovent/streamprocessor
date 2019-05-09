package com.trovent.streamprocessor.test.rest;

import static org.junit.jupiter.api.Assertions.fail;

import javax.ws.rs.client.WebTarget;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.restapi.ApplicationServer;

public class TestKafkaService {
	ApplicationServer server;
	WebTarget target;

	EplStatement stmtTwo;

	@BeforeEach
	protected void setUp() {

	}

	@AfterEach
	protected void tearDown() {
		server.stop();
	}

	@Test
	void test() {
		fail("not implemented yet");
	}
}
