package com.trovent.streamprocessor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.kafka.IProducer;

public class OutputProcessor {
		
	private Logger logger = LogManager.getLogger();
	
	String destination;
	
	public OutputProcessor() {
		this.destination = "";
	}
	
	public OutputProcessor(String destination) {
		this.destination = destination;
	}
	
	public void process(IProducer producer, EventBean[] events, String eventType) {
		for (EventBean eb : events) {
			EplEvent event = new EplEvent(eb.getEventType().getName());

			for (EventPropertyDescriptor descriptor : eb.getEventType().getPropertyDescriptors()) {
				String propName = descriptor.getPropertyName();
				event.add(propName, eb.get(propName));
			}

			String jsonString;
			try {
				jsonString = event.toJson(destination);
				logger.debug("{} event: {}", eventType, jsonString);
				producer.send(jsonString);
			} catch (JsonProcessingException e) {
				logger.error("Error converting event into json format");
				logger.error(e.getMessage());
			}
		}
	}

}
