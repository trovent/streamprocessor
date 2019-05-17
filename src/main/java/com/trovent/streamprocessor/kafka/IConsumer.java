package com.trovent.streamprocessor.kafka;

import java.time.Duration;
import java.util.List;

public interface IConsumer {
	public List<String> poll(Duration duration);
}
