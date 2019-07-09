package com.trovent.streamprocessor.restapi;

import io.swagger.annotations.ApiModelProperty;

public class ProducerConnector {

	@ApiModelProperty(notes = "Name of the kafka topic that is written into", example = "ntp_events")
	public String topic;
	@ApiModelProperty(notes = "Name of the esper statement that shall be connected to kafka topic", example = "ntp_filter")
	public String eplStatementName;
	@ApiModelProperty(notes = "Name of the key where data will be stored", example = "connection")
	public String destination;

	public ProducerConnector() {

	}

	public ProducerConnector(String topic, String eplStatementName) {
		this.topic = topic;
		this.eplStatementName = eplStatementName;
	}
	
	public ProducerConnector(String topic, String eplStatementName, String destination) {
		this.topic = topic;
		this.eplStatementName = eplStatementName;
		this.destination = destination;
	}
}
