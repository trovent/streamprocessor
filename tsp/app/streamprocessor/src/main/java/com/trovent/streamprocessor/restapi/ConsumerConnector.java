package com.trovent.streamprocessor.restapi;

import io.swagger.annotations.ApiModelProperty;

public class ConsumerConnector {

	@ApiModelProperty(notes = "Name of the kafka topic that is read from", example = "windows_syslog")
	public String topic;
	@ApiModelProperty(notes = "Name of the schema/event that shall be connected to kafka topic", example = "syslog")
	public String schemaName;
	@ApiModelProperty(notes = "Name of the key from which data needs to be processed", example = "syslog")
	public String source;

	ConsumerConnector() {

	}

	public ConsumerConnector(String topic, String schemaName) {
		this.topic = topic;
		this.schemaName = schemaName;
	}
	
	public ConsumerConnector(String topic, String schemaName, String source) {
		this.topic = topic;
		this.schemaName = schemaName;
		this.source = source;
	}
}
