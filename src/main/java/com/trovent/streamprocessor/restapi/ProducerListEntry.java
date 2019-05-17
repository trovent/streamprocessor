package com.trovent.streamprocessor.restapi;

/**
 * Entity class that is a template for the JSON object returned by GET
 * api/producers
 */
public class ProducerListEntry {

	public int id;
	public ProducerConnector connector;

	ProducerListEntry() {
	}

	ProducerListEntry(int id, ProducerConnector connector) {
		this.id = id;
		this.connector = connector;
	}
}
