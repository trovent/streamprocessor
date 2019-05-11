package com.trovent.streamprocessor.kafka;

import java.util.LinkedList;

public class StringQueueProducer implements IProducer {

	LinkedList<String> queue;

	public StringQueueProducer() {
		this.queue = new LinkedList<>();
	}

	public String poll() {
		return this.queue.poll();
	}

	public int count() {
		return this.queue.size();
	}

	public boolean isEmpty() {
		return this.queue.isEmpty();
	}

	@Override
	public void send(String value) {
		this.queue.add(value);
	}

}
