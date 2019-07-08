package com.trovent.streamprocessor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EventType;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.InputProcessor;

/**
 * This class process an event given as a JSON string and parses it and passes
 * it to the given event type of esper engine.
 * 
 */
abstract public class AbstractInputProcessor implements InputProcessor {

	protected Logger logger;

	protected TSPEngine engine;

	protected EventType eventType;
	
	protected String source;

	/**
	 * Getter for field engine
	 */
	public TSPEngine getEngine() {
		return engine;
	}

	/**
	 * Getter for field eventType
	 */
	public EventType getEventType() {
		return eventType;
	}

	/**
	 * Constructor of AbstractInputProcessor
	 * 
	 * @param engine Sets the engine to use for event processing
	 */
	public AbstractInputProcessor(TSPEngine engine) {
		this.engine = engine;
		this.logger = LogManager.getLogger();
	}

	/**
	 * Constructor of AbstractInputProcessor
	 * 
	 * @param engine        Sets the engine to use for event processing
	 * 
	 * @param eventTypeName Sets the event type to use. The incoming data must match
	 *                      this format.
	 */
	public AbstractInputProcessor(TSPEngine engine, String eventTypeName) throws EPException {
		this.engine = engine;
		this.logger = LogManager.getLogger();

		logger.info("creating JSONINputProcessor for eventType '{}'", eventTypeName);

		this.setEventType(eventTypeName);
	}
	
	/**
	 * Constructor of AbstractInputProcessor
	 * 
	 * @param engine        Sets the engine to use for event processing
	 * 
	 * @param eventTypeName Sets the event type to use. The incoming data must match
	 *                      this format.
	 * @param source		Source key to be used to find data
	 */
	public AbstractInputProcessor(TSPEngine engine, String eventTypeName, String source) throws EPException {
		this.engine = engine;
		this.logger = LogManager.getLogger();

		logger.info("creating JSONINputProcessor for eventType '{}' and source '{}'", eventTypeName, source);

		this.setEventType(eventTypeName);
		this.source = source;
	}

	/**
	 * Set the event type used by the input processor
	 * 
	 * @param eventTypeName Sets the event type to use. The incoming data must match
	 *                      this format.
	 */
	public void setEventType(String eventTypeName) throws EPException {
		this.eventType = null;
		this.eventType = this.engine.getEventType(eventTypeName);
	}
	
	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	/**
	 * Process the incoming data given as string and pass it to the engine.
	 * <p>
	 * <i>- To be implemented by derived classes -</i>
	 * 
	 * @param input The input event given as string
	 */
	@Override
	abstract public Boolean process(String input);
}
