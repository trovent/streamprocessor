package com.trovent.streamprocessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.trovent.streamprocessor.restapi.ApplicationServer;
//import com.sun.tools.doclets.internal.toolkit.util.DocFinder.Input;
import com.trovent.streamprocessor.kafka.InputProcessor;
import com.trovent.streamprocessor.kafka.KafkaManager;


public class Application {

	private Logger logger;
	private TSPEngine engine = new TSPEngine();
	

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
		
		if (config.getConfigfile()!=null) {
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
		
		
		// TEST START
		System.out.println("this is System here: starting TestRun");
		try {
			this.interactiveStatementEntry();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TEST END
		
		/*
		 * init and start esper engine test connection to kafka start application server
		 */

		testConsumer();

		
		ApplicationServer server = new ApplicationServer(this.config);
		server.start();
		
		System.out.println(String.format("Application started and is listening on port %d", this.config.getPort() ));
  
		System.in.read();

		
		this.logger.info("shutting down...");
		
		server.stop();
		this.logger.trace("run() done");
	}

	public static void main(String[] args)  throws IOException {
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
