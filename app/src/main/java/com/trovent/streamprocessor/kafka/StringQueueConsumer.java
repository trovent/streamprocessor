package com.trovent.streamprocessor.kafka;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StringQueueConsumer implements IConsumer {

	ConcurrentLinkedQueue<String> queue;

	public StringQueueConsumer() {
		this.queue = new ConcurrentLinkedQueue<>();
	}

	public void push(String data) throws InterruptedException {
		this.queue.add(data);
	}

	@Override
	final public List<String> poll(Duration duration) {
		List<String> data = new LinkedList<String>();
		if (!this.queue.isEmpty()) {
			data.add(this.queue.poll());
		}
		return data;
	}

}
