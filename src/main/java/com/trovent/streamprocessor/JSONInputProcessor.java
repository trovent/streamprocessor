package com.trovent.streamprocessor;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.trovent.streamprocessor.esper.TSPEngine;

/**
 * This class process an event given as a JSON string and parses it and passes
 * it to the given event type of esper engine.
 * 
 */
public class JSONInputProcessor extends AbstractInputProcessor {

	/**
	 * Constructor of JSONInputProcessor
	 * 
	 * @param engine Sets the engine to use for event processing
	 */
	public JSONInputProcessor(TSPEngine engine) {
		super(engine);
	}

	/**
	 * Constructor of JSONInputProcessor
	 * 
	 * @param engine        Sets the engine to use for event processing
	 * 
	 * @param eventTypeName Sets the event type to use. The incoming data must match
	 *                      this format.
	 */
	public JSONInputProcessor(TSPEngine engine, String eventTypeName) throws EPException {
		super(engine, eventTypeName);
	}

	/**
	 * Process the incoming data given as Map. If the format matches the previously
	 * set event type, the data is transformed into an esper event and sent to the
	 * engine. The method fails and returns true if it detects a type mismatch or
	 * required fields are missing.
	 * 
	 * @param input The input event given as key-value pairs
	 * @return true if the event was processed successfully, false otherwise
	 */
	public Boolean process(Map<String, String> input) {
		Map<String, Object> event = new HashMap<String, Object>();
		for (EventPropertyDescriptor descriptor : this.eventType.getPropertyDescriptors()) {
			String propName = descriptor.getPropertyName();
			Class<?> propType = descriptor.getPropertyType();

			if (!input.containsKey(propName)) {
				logger.warn("cannot process '{}' - field '{}' is missing", input, propName);
				return false;
			}

			String value = input.get(propName);
			try {
				if (propType == String.class) {
					event.put(propName, value);
				} else if (propType == Integer.class) {
					event.put(propName, new Integer(value));
				} else if (propType == Boolean.class) {
					event.put(propName, new Boolean(value));
				} else if (propType == Float.class) {
					event.put(propName, new Float(value));
				} else if (propType == Double.class) {
					event.put(propName, new Double(value));
				} else if (propType == Long.class) {
					event.put(propName, new Long(value));
				} else if (propType == Byte.class) {
					event.put(propName, new Byte(value));
				} else if (propType == BigInteger.class) {
					event.put(propName, new BigInteger(value));
				} else if (propType == BigDecimal.class) {
					event.put(propName, new BigDecimal(value));
				}
			} catch (NumberFormatException e) {
				logger.warn("type mismatch for value '{}' of field '{}' - could not convert to {}", value, propName,
						propType.toString());
				return false;
			}

		}
		this.engine.sendEPLEvent(this.eventType.getName(), event);
		return true;
	}

	/**
	 * Process the incoming data given as json string. The json string is parsed and
	 * transformed into Map<String, String>. If the format matches the previously
	 * set event type, the data is transformed into an esper event and sent to the
	 * engine.
	 * 
	 * @param input The input event given json string e.g.
	 *              <code>{ "name" : "Trovent",
	 *              "year" : 2019 }</code>
	 * @return true if the event was processed successfully, false otherwise
	 */
	@Override
	public Boolean process(String input) {

		this.logger.debug(String.format("input: '%s'", input));

		Type mapToken = new TypeToken<Map<String, String>>() {
		}.getType();

		Gson gson = new Gson();
		try {
			Map<String, String> inputAsMap = gson.fromJson(input, mapToken);
			return this.process(inputAsMap);
		} catch (JsonSyntaxException e) {
			this.logger.warn(e);
			this.logger.warn("input: {}", input);
			return false;
		}

	}
}
