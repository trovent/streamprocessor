package com.trovent.streamprocessor.restapi;

public class Connector {
	public String topic;
	public String schema;

	public Connector() {
	}

	public Connector(String topic, String schema) {
		this.topic = topic;
		this.schema = schema;
	}
}
