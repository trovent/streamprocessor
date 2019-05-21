package com.trovent.streamprocessor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.trovent.streamprocessor.restapi.ApplicationServer;

public class Application {

	private Logger logger;

	final private String DEFAULTCONFIGFILE = "app.properties";

	private Configuration config;

	private static Application instance;

	public static Application getInstance() {
		if (instance == null) {
			instance = new Application();
		}
		return instance;
	}

	public Configuration getConfig() {
		return this.config;
	}

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
			try {
				config.readConfigFile(new FileInputStream(f));
			} catch (FileNotFoundException e) {
				this.logger.error("config file {} does not exist!", config.getConfigfile());
				// read default properties file
				config.readConfigFile(this.getClass().getClassLoader().getResourceAsStream(DEFAULTCONFIGFILE));
			}
		} else {
			// read default properties file
			config.readConfigFile(this.getClass().getClassLoader().getResourceAsStream(DEFAULTCONFIGFILE));
		}

		this.logger.trace("init() done");
	}

	private void run() {
		this.logger.trace("entering run()");
		this.logger.info("starting Trovent Stream Processor");

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
		Application app = Application.getInstance();
		app.init(args);
		app.run();
	}
}
