package com.trovent.streamprocessor.restapi;

import io.swagger.annotations.ApiModelProperty;

/**
 * Entity class that is a template for the JSON object returned by GET
 * api/producers
 */
public class ProducerListEntry {

	@ApiModelProperty(notes = "Unique id of the producer", example = "666")
	public int id;
	public ProducerConnector connector;

	ProducerListEntry() {
	}

	public ProducerListEntry(int id, ProducerConnector connector) {
		this.id = id;
		this.connector = connector;
	}
}
