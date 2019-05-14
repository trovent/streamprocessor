package com.trovent.streamprocessor.kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.espertech.esper.client.UpdateListener;
import com.google.gson.Gson;
import com.trovent.streamprocessor.esper.EplEvent;

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

	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {

		for (EventBean eb : newEvents) {
			EplEvent event = new EplEvent(eb.getEventType().getName());
			for (EventPropertyDescriptor descriptor : eb.getEventType().getPropertyDescriptors()) {
				String propName = descriptor.getPropertyName();
				event.add(propName, eb.get(propName));
			}
			String jsonString = new Gson().toJson(event);

			logger.info(jsonString);
			this.producer.send(jsonString);
		}

	}

	public String getStatementName() {
		return statementName;
	}

	public void setStatementName(String statementName) {
		this.statementName = statementName;
	}

}
