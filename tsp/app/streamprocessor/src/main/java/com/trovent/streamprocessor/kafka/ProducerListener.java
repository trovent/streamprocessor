package com.trovent.streamprocessor.kafka;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.trovent.streamprocessor.OutputProcessor;
import com.trovent.streamprocessor.restapi.ProducerConnector;

public class ProducerListener implements UpdateListener {

	private IProducer producer;

	private String statementName;
	
	private OutputProcessor outputProcessor;

	public ProducerListener(IProducer producer) {
		this.producer = producer;
		this.outputProcessor = new OutputProcessor();
	}

	public ProducerListener(IProducer producer, String statementName) {
		this.producer = producer;
		this.setStatementName(statementName);
		this.outputProcessor = new OutputProcessor();
	}
	
	public ProducerListener(IProducer producer, String statementName, OutputProcessor output) {
		this.producer = producer;
		this.setStatementName(statementName);
		this.outputProcessor =  output;
	}

	public IProducer getProducer() {
		return this.producer;
	}

	public ProducerConnector getConnector() {
		String topic = "";
		if (producer instanceof TSPKafkaProducer) {
			topic = ((TSPKafkaProducer) this.producer).getTopic();
		}
		return new ProducerConnector(topic, this.statementName, this.outputProcessor.getDestination());
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		if (newEvents != null) {
			outputProcessor.process(this.producer, newEvents, "ISTREAM");
		}
		if (oldEvents != null) {
			outputProcessor.process(this.producer, oldEvents, "RSTREAM");
		}
	}

	public String getStatementName() {
		return statementName;
	}

	public void setStatementName(String statementName) {
		this.statementName = statementName;
	}

}
