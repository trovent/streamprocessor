package com.trovent.streamprocessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.trovent.streamprocessor.restapi.ApplicationServer;
import com.sun.tools.doclets.internal.toolkit.util.DocFinder.Input;


public class Application {

	private Logger logger;
	private TSPEngine engine = new TSPEngine();
	
	final private String defaultConfigFile = "app.properties";
	
	private Configuration config;
	/**
	 * Initialise application
	 * @param args
	 */	
	private void init(String[] args) {

		// TODO
		/*
		 * parse commandline parameters
		 * open configfile, read settings
		 * initialise logging component
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
		}
		else {
			// read default properties file
			config.readConfigFile(defaultConfigFile);
		}
		
		this.logger.trace("init() done");
	}
	
	
	private void run() throws IOException {
		this.logger.trace("entering run()");
		this.logger.info("starting Trovent Stream Processor");
		
		
		// TEST START
		System.out.println("this is System here: starting TestRun");
		try {
			this.statementTestRun();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TEST END
		
		/*
		 * init and start esper engine
		 * test connection to kafka
		 * start application server
		 */
		
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
	
	void statementTestRun() throws IOException {
		engine.init();
		String statement;
		statement = "create objectarray schema SomeArrayEventSchema as (first_name string, numbers integer)";
		engine.addEPLStatement(statement, "ArraySchema");
		statement = "select first_name as First_Name from SomeArrayEventSchema";
		engine.addEPLStatement(statement, "ArrayStatement");
		statement = "create map schema SomeMapEventSchema as (carId integer, fictionalCarType String)";
		engine.addEPLStatement(statement, "MapSchema");
		
		
		//testData as objectarray
		// "one";"many words";987;"small"
		// [ "one", "many words", 987, "small" ]
		Object[] objArrayData = new Object[2];
		objArrayData[0] = new String("hello");
		objArrayData[1] = new Integer(666);
		
		//testData as Map
		// { "cardId" : 1230,
		//	 "fictionalCarType" : "gravida" }
		Map<String, Object> mapData = new HashMap<String, Object>();
		mapData.put("carId", 1230);
		mapData.put("fictionalCarType", "gravida");
		
		engine.sendEPLEvent("SomeArrayEventSchema", objArrayData);
		engine.sendEPLEvent("SomeMapEventSchema", mapData);
		
		System.out.println("Enter custom statements now. Type exit to exit");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		//tests for custom input and sends it as a statement
		System.out.print("Enter new Statement");
        String s = br.readLine();
		while(s.equals("exit")==false) {
			engine.addEPLStatement(s, "inputStatement");
			System.out.print("Enter new Statement");
			s = br.readLine();
		}
	}
	
	
}
