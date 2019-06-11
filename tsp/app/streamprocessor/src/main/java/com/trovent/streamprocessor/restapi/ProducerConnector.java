package com.trovent.streamprocessor.restapi;

import io.swagger.annotations.ApiModelProperty;

public class ProducerConnector {

	@ApiModelProperty(notes = "Name of the kafka topic that is written into", example = "ntp_events")
	public String topic;
	@ApiModelProperty(notes = "Name of the esper statement that shall be connected to kafka topic", example = "ntp_filter")
	public String eplStatementName;

	public ProducerConnector() {

	}

	public ProducerConnector(String topic, String eplStatementName) {
		this.topic = topic;
		this.eplStatementName = eplStatementName;
	}
}
