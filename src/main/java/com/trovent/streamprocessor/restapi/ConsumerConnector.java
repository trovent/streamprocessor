package com.trovent.streamprocessor.restapi;

public class ConsumerConnector {
	public String topic;
	public String schemaName;

	public ConsumerConnector() {
	}

	public ConsumerConnector(String topic, String schemaName) {
		this.topic = topic;
		this.schemaName = schemaName;
	}
}
