package com.trovent.streamprocessor.restapi;

public class ConsumerListEntry {
	/**
	 * 
	 */
	public int id;
	public ConsumerConnector connector;

	ConsumerListEntry(int id, ConsumerConnector connector) {
		this.id = id;
		this.connector = connector;
	}
}