package com.trovent.streamprocessor.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;

public class StringQueueConsumer implements IConsumer {

	LinkedList<String> queue;

	public StringQueueConsumer() {
		this.queue = new LinkedList<>();
	}

	public void push(String data) {
		this.queue.push(data);
	}

	@Override
	public String[] poll(Duration duration) {
		ArrayList<String> data = new ArrayList<>();
		String value = this.queue.poll();
		if (value != null) {
			data.add(value);
		}
		String array[] = new String[data.size()];
		return data.toArray(array);
	}

}
