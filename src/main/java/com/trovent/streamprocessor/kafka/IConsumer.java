package com.trovent.streamprocessor.kafka;

import java.time.Duration;

public interface IConsumer {
	public String[] poll(Duration duration);
}
