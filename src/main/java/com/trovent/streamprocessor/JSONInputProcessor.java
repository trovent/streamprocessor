package com.trovent.streamprocessor;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.espertech.esper.client.EventType;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.trovent.streamprocessor.kafka.InputProcessor;

public class JSONInputProcessor implements InputProcessor {

	// private EventType eventType;

	// void PlaintextInputProcessor(EsperService service, String eventTypeName) {

	private Logger logger;

	private TSPEngine engine;

	private EventType eventType;

	public TSPEngine getEngine() {
		return engine;
	}

	public EventType getEventType() {
		return eventType;
	}

	public JSONInputProcessor(TSPEngine engine) {
		this.engine = engine;
		this.logger = LogManager.getLogger();
	}

	public JSONInputProcessor(TSPEngine engine, String eventTypeName) throws EPException {
		this.engine = engine;
		this.logger = LogManager.getLogger();

		logger.info("creating JSONINputProcessor for eventType '{}'", eventTypeName);

		this.setEventType(eventTypeName);
	}

	public void setEventType(String eventTypeName) throws EPException {
		this.eventType = this.engine.getEPServiceProvider().getEPAdministrator().getConfiguration()
				.getEventType(eventTypeName);

		if (this.eventType == null) {
			throw new EPException(String.format("EventType '%s' not found", eventTypeName));
		}

		try {
			// dump info about event type
			logger.info("# Definition of event type #");

			EventPropertyDescriptor[] descriptors = this.eventType.getPropertyDescriptors();
			for (EventPropertyDescriptor field : descriptors) {
				logger.info("\tname: {}  \ttype: {}", field.getPropertyName(), field.getPropertyType().toString());
			}

		} catch (Exception e) {
			logger.error("Failed to set eventType '{}'", eventTypeName);
			logger.error(e);
		}
	}

	public Boolean process(Map<String, String> input) {
		Map<String, Object> event = new HashMap<String, Object>();
		for (EventPropertyDescriptor descriptor : this.eventType.getPropertyDescriptors()) {
			String propName = descriptor.getPropertyName();
			Class<?> propType = descriptor.getPropertyType();

			if (!input.containsKey(propName)) {
				logger.warn("cannot process '{}' - field '{}' is missing", input, propName);
				return false;
			}
			if (propType == String.class) {
				event.put(propName, input.get(propName));
			} else if (propType == Integer.class) {
				event.put(propName, new Integer(input.get(propName)));
			} else if (propType == Boolean.class) {
				event.put(propName, new Boolean(input.get(propName)));
			}
			// continue checking other types
		}
		this.engine.sendEPLEvent(this.eventType.getName(), event);
		return true;
	}

	public Boolean process(String input) {

		this.logger.debug(String.format("input: '%s'", input));

		Type mapToken = new TypeToken<Map<String, String>>() {
		}.getType();

		Gson gson = new Gson();
		Map<String, String> inputAsMap = gson.fromJson(input, mapToken);

		return this.process(inputAsMap);

	}
}
