package com.trovent.streamprocessor.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.google.gson.Gson;
import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.ConnectorController;
import com.trovent.streamprocessor.kafka.KafkaManager;
import com.trovent.streamprocessor.kafka.Producer;
import com.trovent.streamprocessor.restapi.ConsumerConnector;

class TestConnectorController {

	TSPEngine engine = TSPEngine.create();

	KafkaManager kafkaManager = new KafkaManager();

	String topic = "input";

	EplSchema schema;
	EplStatement statement;

	@BeforeEach
	void setUp() throws Exception {

		engine.init();

		schema = new EplSchema("employees");
		schema.add("name", "string").add("length", "integer").add("isMale", "boolean");

		statement = new EplStatement("FilterNewbies", "select * from employees where length<2");

		engine.addEPLSchema(schema);
		engine.addEPLStatement(statement);

		// InputProcessor input = new JSONInputProcessor(this.engine, schema.name);

		// Consumer c = this.kafkaManager.createConsumer(topic, input);
		// Producer p = this.kafkaManager.createProducer(topic);
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testA() throws InterruptedException {
		ConnectorController controller = new ConnectorController(this.engine, this.kafkaManager);

		ConsumerConnector connector = new ConsumerConnector(topic, schema.name);
		controller.connect(connector);

		// now kafka topic "input" is connector to schema "employees"
		// => consumer thread reads data from kafka into esper schema
		// TODO: put data into kafka => create producer manually
		// TODO: check that data is comming in => create and add Listener

		class MyListener implements UpdateListener {
			public int counter = 0;

			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				for (EventBean event : newEvents) {
					System.out.println(event.toString());
					counter++;
				}
			}
		}

		MyListener listener = new MyListener();
		this.engine.addListener(statement.name, listener);

		Producer producer = this.kafkaManager.createProducer(topic);

		Gson gson = new Gson();
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("name", "John");
		data.put("length", 1);
		data.put("isMale", true);
		producer.send(gson.toJson(data));

		while (listener.counter == 0) {
			System.out.println("waiting...");
		}
	}

}
