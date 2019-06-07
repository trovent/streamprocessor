package com.trovent.streamprocessor.restapi;

import io.swagger.annotations.ApiModelProperty;

/**
 * Entity class that is a template for the JSON object returned by GET
 * api/consumers
 */
public class ConsumerListEntry {

	@ApiModelProperty(notes = "Unique id of the consumer", example = "4711")
	public int id;
	public ConsumerConnector connector;

	ConsumerListEntry() {
	}

	public ConsumerListEntry(int id, ConsumerConnector connector) {
		this.id = id;
		this.connector = connector;
	}
}