package com.trovent.streamprocessor;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.trovent.streamprocessor.kafka.InputProcessor;
import com.trovent.streamprocessor.kafka.KafkaManager;

public class Application {

	private Logger logger;

	private KafkaManager kafkaManager;

	final private String defaultConfigFile = "app.properties";

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

		Configuration config = new Configuration();
		config.parseArguments(args);

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
		// TODO
		/*
		 * init and start esper engine test connection to kafka start application server
		 */

		testConsumer();

		this.logger.info("shutting down...");
		this.logger.trace("run() done");
	}

	public static void main(String[] args) {
		Application app = new Application();
		app.init(args);
		app.run();
	}

	private void testConsumer() {

		// InputProcessor input = new CSVInputProcessor("myevent");
		InputProcessor input = new JSONInputProcessor("myevent");

		this.kafkaManager.createConsumer("syslog", input);
		// this.kafkaManager.createConsumer("netflow", input);
	}
}
