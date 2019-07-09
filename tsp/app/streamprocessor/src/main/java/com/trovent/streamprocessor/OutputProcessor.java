package com.trovent.streamprocessor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.esper.serializer.EplEventSerializer;
import com.trovent.streamprocessor.kafka.IProducer;

public class OutputProcessor {
		
	private Logger logger = LogManager.getLogger();
	
	String destination;
	
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
				if (this.destination == null || this.destination.equals("")) {
					jsonString = event.toJson();
				} else {
					// Prepare serializer for event object
					EplEventSerializer eplEventSerializer = new EplEventSerializer();
					eplEventSerializer.setDestination(this.destination);
					
					ObjectMapper mapper = new ObjectMapper();
					
					// Prepare module for mapper to use serializer
					SimpleModule module = new SimpleModule();
					module.addSerializer(EplEvent.class, eplEventSerializer);
					mapper.registerModule(module);
					
					// Convert event object to json string
					jsonString = mapper.writeValueAsString(event);
				}
				logger.debug("{} event: {}", eventType, jsonString);
				producer.send(jsonString);
			} catch (JsonProcessingException e) {
				logger.error("Error converting event into json format");
				logger.error(e.getMessage());
			}
		}
	}

}
