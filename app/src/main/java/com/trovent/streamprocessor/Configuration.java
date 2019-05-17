package com.trovent.streamprocessor;

import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class Configuration {
	
	private CmdLineParser parser;
	
	private Logger logger;
	
	
	@Option(name="-c", usage="Sets config file name")
	public String configfile;
	

	private int port;
	
	private String kafkaAddress;
	
	
	public String getConfigfile() {
		return configfile;
	}
	
	public int getPort() {
		return port;
	}

	public String getKafkaAddress() {
		return kafkaAddress;
	}
	
	
	
	public Configuration() {
		
		parser = new CmdLineParser(this);			
		logger = LogManager.getLogger();
	}
	
	public void parseArguments(String[] args) {
		
		try {
			parser.parseArgument(args);
					
		} catch (CmdLineException e) {		
			logger.error(e.getMessage());
		}
		
	}
	
	private void parseProperties(Properties prop)
	{
		// TODO: read config options from properties
		logger.trace("parsing Properties");
		
		prop.forEach( (key, value ) -> { logger.debug("config property:  {} = {}",  key,  value); } );
		
		kafkaAddress = prop.getProperty("kafka.address", "http://localhost:9092");
		port = Integer.parseInt( prop.getProperty("common.port", "8080") );
		
	}
	
	public void readConfigFile(String configFileName)
	{
		Properties prop = new Properties();
		try {
			logger.trace("trying to read config from file: {}", configFileName);
			// find file 'app.properties' in resources
			prop.load(this.getClass().getClassLoader().getResourceAsStream(configFileName));
			
			parseProperties(prop);
			
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
}
