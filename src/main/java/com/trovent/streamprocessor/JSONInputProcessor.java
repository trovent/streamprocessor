package com.trovent.streamprocessor;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.trovent.streamprocessor.kafka.InputProcessor;

public class JSONInputProcessor implements InputProcessor {

	// private EventType eventType;

	// void PlaintextInputProcessor(EsperService service, String eventTypeName) {

	private Logger logger;

	JSONInputProcessor(String eventTypeName) {
		// this.eventType =
		// engine.getEPAdministrator().getConfiguration().getEventType(eventTypeName) }
		this.logger = LogManager.getLogger();
	}

	public void process(String input) {

		this.logger.debug(String.format("input: '%s'", input));

		Type mapToken = new TypeToken<Map<String, String>>() {
		}.getType();

		Gson gson = new Gson();
		Map<String, String> m = gson.fromJson(input, mapToken);
		this.logger.debug(String.format("parsed: '%s'", m.toString()));

		Map map = gson.fromJson(input, Map.class);
		this.logger.debug(String.format("parsed: '%s'", map.toString()));

		map.forEach((k, v) -> {
			System.out.println(String.format("%s : %s (%s)", k, v, v.getClass()));

		});

		/*
		 * Gson gson = new GsonBuilder().create(); gson.toJson("Hello", System.out);
		 * gson.toJson(123, System.out);
		 */
		// TODO: read from given string, data is given as csv
		// parse values (split at ',')
		// create an esper event (format given by eventType)
		// => use TSPEngine
		// send event ( this.engine.getEPRuntime().sendEvent(ev, "PersonEvent"); )
		// => use TSPEngine
	}
}
