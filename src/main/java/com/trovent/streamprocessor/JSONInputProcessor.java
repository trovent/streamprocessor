package com.trovent.streamprocessor;

import java.io.IOException;

import com.espertech.esper.client.EPException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.trovent.streamprocessor.esper.EplEvent;
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

		try {
			EplEvent event = new EplEvent(this.eventType.getName());
			event.dataFromJson(input);

			this.engine.sendEPLEvent(event);
		} catch (JsonParseException e1) {
			this.logger.warn(e1.getMessage());
			return false;
		} catch (JsonMappingException e1) {
			this.logger.warn(e1.getMessage());
			return false;
		} catch (IOException e1) {
			this.logger.warn(e1.getMessage());
			return false;
		}

		return true;
	}
}
