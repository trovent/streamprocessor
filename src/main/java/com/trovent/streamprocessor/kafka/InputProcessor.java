package com.trovent.streamprocessor.kafka;

public interface InputProcessor {

	/*
	 * Process the given input string
	 */
	void process(String input);

}
