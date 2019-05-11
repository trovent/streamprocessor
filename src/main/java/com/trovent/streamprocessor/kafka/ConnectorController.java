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

	Set<ConsumerThread> consumerThreads;
	Set<ProducerListener> listeners;

	public ConnectorController(TSPEngine engine, KafkaManager kafkaManager) {
		this.engine = engine;
		this.kafkaManager = kafkaManager;

		this.consumerThreads = new HashSet<ConsumerThread>();
		this.listeners = new HashSet<ProducerListener>();
	}

	public ConsumerThread getConsumerThread(int hashCode) {
		for (ConsumerThread consumerThread : consumerThreads) {
			if (consumerThread.hashCode() == hashCode) {
				return consumerThread;
			}
		}
		return null;
	}

	public ProducerListener getListener(int hashCode) {
		for (ProducerListener listener : listeners) {
			if (listener.hashCode() == hashCode) {
				return listener;
			}
		}
		return null;
	}

	public int connect(ConsumerConnector connector) {
		InputProcessor input = new JSONInputProcessor(this.engine, connector.schemaName);
		ConsumerThread consumerThread;
		if (connector.topic != null) {
			consumerThread = this.kafkaManager.createConsumerThread(connector.topic, input);
		} else {
			IConsumer consumer = new StringQueueConsumer();
			consumerThread = this.kafkaManager.createConsumerThread(consumer, input);
		}

		this.consumerThreads.add(consumerThread);
		return consumerThread.hashCode();
	}

	public int connect(ProducerConnector connector) {
		IProducer producer;
		if (connector.topic != null) {
			producer = this.kafkaManager.createProducer(connector.topic);
		} else {
			producer = new StringQueueProducer();
		}
		ProducerListener listener = new ProducerListener(producer, connector.eplStatementName);

		this.engine.addListener(connector.eplStatementName, listener);
		this.listeners.add(listener);
		return listener.hashCode();
	}

	public void disconnect(int id) {
		this.consumerThreads.forEach((consumer) -> {
			if (consumer.hashCode() == id) {
				this.consumerThreads.remove(consumer);
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
