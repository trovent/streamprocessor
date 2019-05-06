package com.trovent.streamprocessor.restapi;

public class ProducerConnector {
	public String topic;
	public String eplStatementName;

	public ProducerConnector() {
	}

	public ProducerConnector(String topic, String eplStatementName) {
		this.topic = topic;
		this.eplStatementName = eplStatementName;
	}
}
