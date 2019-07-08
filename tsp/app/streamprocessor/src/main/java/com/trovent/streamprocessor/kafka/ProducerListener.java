package com.trovent.streamprocessor.kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.espertech.esper.client.UpdateListener;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.restapi.ProducerConnector;

public class ProducerListener implements UpdateListener {

	private IProducer producer;

	private Logger logger;

	private String statementName;

	public ProducerListener(IProducer producer) {
		this.producer = producer;
		this.logger = LogManager.getLogger();
	}

	public ProducerListener(IProducer producer, String statementName) {
		this.producer = producer;
		this.logger = LogManager.getLogger();
		this.setStatementName(statementName);
	}

	public IProducer getProducer() {
		return this.producer;
	}

	public ProducerConnector getConnector() {
		String topic = "";
		if (producer instanceof TSPKafkaProducer) {
			topic = ((TSPKafkaProducer) this.producer).getTopic();
		}
		return new ProducerConnector(topic, this.statementName);
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {

		if (newEvents != null) {
			for (EventBean eb : newEvents) {
				EplEvent event = new EplEvent(eb.getEventType().getName());

				for (EventPropertyDescriptor descriptor : eb.getEventType().getPropertyDescriptors()) {
					String propName = descriptor.getPropertyName();
					event.add(propName, eb.get(propName));
				}

				String jsonString;
				try {
					jsonString = event.toJson();
					logger.debug("ISTREAM event: {}", jsonString);
					this.producer.send(jsonString);
				} catch (JsonProcessingException e) {
					logger.error("Error converting event into json format");
					logger.error(e.getMessage());
				}
			}
		}

		if (oldEvents != null) {
			for (EventBean eb : oldEvents) {
				EplEvent event = new EplEvent(eb.getEventType().getName());

				for (EventPropertyDescriptor descriptor : eb.getEventType().getPropertyDescriptors()) {
					String propName = descriptor.getPropertyName();
					event.add(propName, eb.get(propName));
				}

				String jsonString;
				try {
					jsonString = event.toJson();
					logger.debug("RSTREAM event: {}", jsonString);
					this.producer.send(jsonString);
				} catch (JsonProcessingException e) {
					logger.error("Error converting event into json format");
					logger.error(e.getMessage());
				}
			}
		}
	}

	public String getStatementName() {
		return statementName;
	}

	public void setStatementName(String statementName) {
		this.statementName = statementName;
	}

}
