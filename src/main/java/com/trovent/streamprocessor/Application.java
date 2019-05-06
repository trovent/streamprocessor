package com.trovent.streamprocessor;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.trovent.streamprocessor.esper.TSPEngine;
import com.trovent.streamprocessor.kafka.InputProcessor;
import com.trovent.streamprocessor.kafka.KafkaManager;
import com.trovent.streamprocessor.kafka.Producer;
import com.trovent.streamprocessor.restapi.ApplicationServer;

public class Application {

	private Logger logger;
	private TSPEngine engine = TSPEngine.create();

	private KafkaManager kafkaManager;

	final private String defaultConfigFile = "app.properties";

	private Configuration config;

	/**
	 * Initialise application
	 * 
	 * @param args
	 */
	private void init(String[] args) {

		// TODO
		/*
		 * parse commandline parameters open configfile, read settings initialise
		 * logging component
		 * 
		 */

		this.logger = LogManager.getLogger();
		this.logger.trace("entering init()");

		this.config = new Configuration();
		this.config.parseArguments(args);

		if (config.getConfigfile() != null) {
			this.logger.debug("reading config from: {}", config.getConfigfile());
			File f = new File(config.getConfigfile());
			if (!f.exists()) {
				this.logger.error("config file {} does not exist!", config.getConfigfile());
				// read default properties file
				config.readConfigFile(defaultConfigFile);
			}
		} else {
			// read default properties file
			config.readConfigFile(defaultConfigFile);
		}

		this.logger.info("creating KafkaManager");
		this.kafkaManager = new KafkaManager();

		this.logger.trace("init() done");
	}

	private void run() {
		this.logger.trace("entering run()");
		this.logger.info("starting Trovent Stream Processor");

		/*
		 * init and start esper engine test connection to kafka start application server
		 */
		this.engine.init();
		createKafkaConsumerDemo();

		ApplicationServer server = new ApplicationServer(this.config);
		server.start();

		System.out.println(String.format("Application started and is listening on port %d", this.config.getPort()));

		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.logger.info("shutting down...");

		server.stop();
		this.logger.trace("run() done");
	}

	public static void main(String[] args) throws IOException {
		Application app = new Application();
		app.init(args);
		app.run();
	}

	private void createKafkaConsumerDemo() {

		// topic where to read from
		String topic = "syslog";
		// event/schema where to write to
		String eventName = "SyslogEvent";

		Map<String, String> syslogEvent = new HashMap<String, String>();
		syslogEvent.put("syslog_timestamp", "string");
		syslogEvent.put("syslog_app", "string");
		syslogEvent.put("syslog_message", "string");
		syslogEvent.put("syslog_pid", "int");

		this.engine.addEPLSchema(eventName, syslogEvent);

		// statement that processes the incoming events
		String statement = "select syslog_timestamp, syslog_app, syslog_message, count(*) from " + eventName
				+ "#time(60)";
		String statementName = "stmtSyslog";

		this.engine.addEPLStatement(statement, statementName);

		// connect event/schema with Kafka topic
		InputProcessor input = new JSONInputProcessor(this.engine, eventName);
		this.kafkaManager.createConsumer(topic, input);

		// define Listener that outputs the events from the statement
		class MyListener implements UpdateListener {

			Producer kafkaProducer;

			MyListener(Producer producer) {
				this.kafkaProducer = producer;
			}

			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				for (EventBean eb : newEvents) {
					logger.info("{} {} {}  count:{}", eb.get("syslog_timestamp"), eb.get("syslog_app"),
							eb.get("syslog_message"), eb.get("count(*)"));

					this.kafkaProducer.send(String.format("%s %s %s %d", eb.get("syslog_timestamp"),
							eb.get("syslog_app"), eb.get("syslog_message"), eb.get("count(*)")));
				}
			}
		}

		this.engine.getEPServiceProvider().getEPAdministrator().getStatement(statementName)
				.addListener(new MyListener(this.kafkaManager.createProducer("output")));

	}
}
