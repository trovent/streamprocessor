package com.trovent.streamprocessor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.trovent.streamprocessor.kafka.InputProcessor;

public class CSVInputProcessor implements InputProcessor {

	// private EventType eventType;

	// void PlaintextInputProcessor(EsperService service, String eventTypeName) {

	private Logger logger;

	CSVInputProcessor(String eventTypeName) {
		// this.eventType =
		// engine.getEPAdministrator().getConfiguration().getEventType(eventTypeName) }
		this.logger = LogManager.getLogger();
	}

	public void process(String input) {

		this.logger.debug(String.format("input: '%s'", input));

		// TODO: read from given string, data is given as csv
		// parse values (split at ',')
		// create an esper event (format given by eventType)
		// => use TSPEngine
		// send event ( this.engine.getEPRuntime().sendEvent(ev, "PersonEvent"); )
		// => use TSPEngine
	}
}
