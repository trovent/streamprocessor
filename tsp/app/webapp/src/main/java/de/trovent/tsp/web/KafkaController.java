package de.trovent.tsp.web;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.ConnectorController;
import com.trovent.streamprocessor.kafka.KafkaManager;
import com.trovent.streamprocessor.restapi.ConsumerConnector;
import com.trovent.streamprocessor.restapi.ConsumerListEntry;
import com.trovent.streamprocessor.restapi.ProducerConnector;
import com.trovent.streamprocessor.restapi.ProducerListEntry;

import de.trovent.tsp.web.dto.AppStatus;

@RestController
@RequestMapping(value = "kafka")
public class KafkaController {
	
	@Value( "${kafka.configFileLocation:}" )
	private String kafkaConfigFileLocation;
	
	private Logger logger = LogManager.getLogger();

	private ConnectorController connectorController;
	
	@PostConstruct
	public void init(){
		if (connectorController == null) {
			TSPEngine engine = TSPEngine.create();
			KafkaManager kafkaManager = null;
			if (kafkaConfigFileLocation.isEmpty())
				kafkaManager = new KafkaManager();
			else
				kafkaManager = new KafkaManager(kafkaConfigFileLocation);
			connectorController = ConnectorController.create(engine, kafkaManager);
		}
	}
	
	@RequestMapping(
			value="status", 
			method = RequestMethod.GET, 
			headers = "Accept=application/json")
	public AppStatus status() {
		logger.info("Getting status of the Kafka Rest Controller");
		return new AppStatus("Kafka", "OK");
	}

	@RequestMapping(
			value = "consumers", 
			method = RequestMethod.GET, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.OK)
	public List<ConsumerListEntry> getConsumers() {

		Map<Integer, ConsumerConnector> connectors = connectorController.getConsumers();

		// Transform into JSON compatible format
		List<ConsumerListEntry> consumerList = new ArrayList<ConsumerListEntry>();
		connectors.forEach((id, connector) -> consumerList.add(new ConsumerListEntry(id, connector)));

		return consumerList;
	}

	@RequestMapping(
			value = "producers", 
			method = RequestMethod.GET, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.OK)
	public List<ProducerListEntry> getProducers() {

		Map<Integer, ProducerConnector> producers = connectorController.getProducers();

		// Transform into JSON compatible format
		List<ProducerListEntry> producerList = new ArrayList<>();
		producers.forEach((id, connector) -> producerList.add(new ProducerListEntry(id, connector)));

		return producerList;
	}

	@RequestMapping(
			value = "consumer", 
			method = RequestMethod.POST, 
			headers = "Accept=application/json")
	public int addConsumer(@RequestBody ConsumerConnector connector) {
		return connectorController.connect(connector);
	}

	@RequestMapping(
			value = "producer", 
			method = RequestMethod.POST, 
			headers = "Accept=application/json")
	public int addProducer(@RequestBody ProducerConnector connector) {
		return connectorController.connect(connector);
	}

	@RequestMapping(
			value = "consumer/{id}", 
			method = RequestMethod.DELETE, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.NO_CONTENT)
	public void deleteConsumer(@PathVariable("id") int id) {
		if (!connectorController.disconnectConsumer(id)) {
			throw new IllegalArgumentException();
		}
	}

	@RequestMapping(
			value = "producer/{id}", 
			method = RequestMethod.DELETE, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.NO_CONTENT)
	public void deleteProducer(@PathVariable("id") int id) {
		if (!connectorController.disconnectProducer(id)) {
			throw new IllegalArgumentException();
		}
	}

}
