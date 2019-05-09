package com.trovent.streamprocessor.kafka;

import java.util.HashSet;
import java.util.Set;

import com.trovent.streamprocessor.JSONInputProcessor;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.restapi.ConsumerConnector;
import com.trovent.streamprocessor.restapi.ProducerConnector;

public class ConnectorController {

	private TSPEngine engine;
	private KafkaManager kafkaManager;

	Set<Consumer> consumers;
	Set<KafkaProducerListener> listeners;

	public ConnectorController(TSPEngine engine, KafkaManager kafkaManager) {
		this.engine = engine;
		this.kafkaManager = kafkaManager;

		this.consumers = new HashSet<Consumer>();
		this.listeners = new HashSet<KafkaProducerListener>();
	}

	public int connect(ConsumerConnector connector) {
		InputProcessor input = new JSONInputProcessor(this.engine, connector.schemaName);
		Consumer consumer = this.kafkaManager.createConsumer(connector.topic, input);

		this.consumers.add(consumer);
		return consumer.hashCode();
	}

	public int connect(ProducerConnector connector) {
		Producer producer = this.kafkaManager.createProducer(connector.topic);
		KafkaProducerListener listener = new KafkaProducerListener(producer, connector.eplStatementName);

		this.engine.addListener(connector.eplStatementName, listener);
		this.listeners.add(listener);
		return listener.hashCode();
	}

	public void disconnect(int id) {
		this.consumers.forEach((consumer) -> {
			if (consumer.hashCode() == id) {
				this.consumers.remove(consumer);
				consumer.stop();
				return;
			}
		});

		this.listeners.forEach((listener) -> {
			if (listener.hashCode() == id) {
				this.listeners.remove(listener);
				this.engine.removeListener(listener.getStatementName(), listener);
				return;
			}
		});
	}
}
