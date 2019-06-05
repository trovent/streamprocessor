package com.trovent.streamprocessor.restapi;

/**
 * Entity class that is a template for the JSON object returned by GET
 * api/consumers
 */
public class ConsumerListEntry {

	public int id;
	public ConsumerConnector connector;

	ConsumerListEntry() {
	}

	public ConsumerListEntry(int id, ConsumerConnector connector) {
		this.id = id;
		this.connector = connector;
	}
}