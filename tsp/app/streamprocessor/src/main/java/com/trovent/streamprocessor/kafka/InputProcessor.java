package com.trovent.streamprocessor.kafka;

public interface InputProcessor {

	/*
	 * Process the given input string
	 */
	Boolean process(String input);

}
