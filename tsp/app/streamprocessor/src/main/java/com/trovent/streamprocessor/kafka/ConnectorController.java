package com.trovent.streamprocessor.kafka;

import java.util.HashMap;
import java.util.Map;

import com.trovent.streamprocessor.JSONInputProcessor;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.restapi.ConsumerConnector;
import com.trovent.streamprocessor.restapi.ProducerConnector;

public class ConnectorController {

	static private ConnectorController instance = null;

	private TSPEngine engine;
	private KafkaManager kafkaManager;

	Map<Integer, ConsumerThread> consumerThreads;
	Map<Integer, ProducerListener> listeners;

	/**
	 * Private constructor. Use factory method create() to create a new singleton
	 * instance of this controller class.
	 * 
	 * @param engine       Esper engine to use
	 * @param kafkaManager KafkaManager to use
	 */
	private ConnectorController(TSPEngine engine, KafkaManager kafkaManager) {
		this.engine = engine;
		this.kafkaManager = kafkaManager;

		this.consumerThreads = new HashMap<>();
		this.listeners = new HashMap<>();
	}

	/**
	 * Create an instance of the class and return it.
	 * 
	 * @param engine       Esper engine to use
	 * @param kafkaManager KafkaManager to use
	 * @return Instance of the ConnectorController
	 */
	static public ConnectorController create(TSPEngine engine, KafkaManager kafkaManager) {
		if (instance == null)
			instance = new ConnectorController(engine, kafkaManager);
		return instance;
	}

	/**
	 * Return an instance of ConnectorController (Singleton object)
	 * 
	 * @return Instance of ConnectorController
	 */
	static public ConnectorController getInstance() {
		return instance;
	}

	/**
	 * Get certain consumer thread identified by id. May return null.
	 * 
	 * @param id Id of the consumer
	 * @return consumer object identified by this id
	 */
	public ConsumerThread getConsumerThread(int id) {
		return this.consumerThreads.get(id);
	}

	/**
	 * Get certain listener identified by id. May return null.
	 * 
	 * @param id Id of the listener
	 * @return Listener object identified by this id
	 */
	public ProducerListener getListener(int id) {
		return this.listeners.get(id);
	}

	/**
	 * Connect a Kafka topic as data source to an Esper schema. Topic and schema are
	 * given by an ConsumerConnector object.
	 * 
	 * @param connector ConsumerConnector containing topic and schema name
	 * @return Id of the new Connection
	 */
	public int connect(ConsumerConnector connector) {
		InputProcessor input = new JSONInputProcessor(this.engine, connector.schemaName, connector.source);
		ConsumerThread consumerThread;
		if (connector.topic != null) {
			consumerThread = this.kafkaManager.createConsumerThread(connector.topic, input);
		} else {
			IConsumer consumer = new StringQueueConsumer();
			consumerThread = this.kafkaManager.createConsumerThread(consumer, input);
		}

		this.consumerThreads.put(consumerThread.hashCode(), consumerThread);
		return consumerThread.hashCode();
	}

	/**
	 * Connect an Esper Listener to a Kafka Producer. Topic and statement are given
	 * by a ProducerConnector object.
	 * 
	 * @param connector ProducerConnector containing topic and statement name
	 * @return Id of the new Connection
	 */
	public int connect(ProducerConnector connector) {
		IProducer producer;
		if (connector.topic != null) {
			producer = this.kafkaManager.createProducer(connector.topic);
		} else {
			producer = new StringQueueProducer();
		}
		ProducerListener listener = new ProducerListener(producer, connector.eplStatementName);
		// ProducerListener listener = new ProducerListener(producer, connector.eplStatementName, connector.destination);

		this.engine.addListener(connector.eplStatementName, listener);
		this.listeners.put(listener.hashCode(), listener);
		return listener.hashCode();
	}

	/**
	 * Return a map of all active Consumers connected to a Kafka topic. Key of each
	 * entry is the hashCode of the consumer thread.
	 * 
	 * @return Map containing all HashCode - ConsumerConnector pairs
	 */
	public Map<Integer, ConsumerConnector> getConsumers() {
		Map<Integer, ConsumerConnector> connectors = new HashMap<Integer, ConsumerConnector>();

		for (ConsumerThread thread : this.consumerThreads.values()) {
			connectors.put(thread.hashCode(), thread.getConnector());
		}

		return connectors;
	}

	/**
	 * Return a map of all active Producers connected to a Kafka topic. Key of each
	 * entry is the hashCode of the ProducerListener.
	 * 
	 * @return
	 */
	public Map<Integer, ProducerConnector> getProducers() {
		Map<Integer, ProducerConnector> connectors = new HashMap<Integer, ProducerConnector>();

		for (ProducerListener listener : this.listeners.values()) {
			connectors.put(listener.hashCode(), listener.getConnector());
		}

		return connectors;
	}

	/**
	 * Remove a consumer connection given by id
	 * 
	 * @param id Id of the consumer connection
	 * @return True if a connection was removed, false otherwise
	 */
	public boolean disconnectConsumer(int id) {

		return this.consumerThreads.remove(id) != null;
	}

	/**
	 * Remove a producer connection given by id
	 * 
	 * @param id Id of the producer connection
	 * @return True if a connection was removed, false otherwise
	 */
	public boolean disconnectProducer(int id) {
		return this.listeners.remove(id) != null;
	}
}
