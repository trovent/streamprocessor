package com.trovent.streamprocessor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class Configuration {

	private CmdLineParser parser;

	private Logger logger;

	private Properties properties;

	@Option(name = "-c", usage = "Sets config file name")
	public String configfile;

	private int port;

	public String getConfigfile() {
		return configfile;
	}

	public int getPort() {
		return port;
	}

	public Properties getProperties() {
		return properties;
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

	private void parseProperties(Properties prop) {

		logger.trace("parsing Properties");

		prop.forEach((key, value) -> {
			logger.debug("config property:  {} = {}", key, value);
		});

		port = Integer.parseInt(prop.getProperty("common.port", "8080"));
	}

	public void readConfigFile(InputStream istream) {

		this.properties = new Properties();
		try {
			// find file 'app.properties' in resources
			// properties.load(this.getClass().getClassLoader().getResourceAsStream(configFileName));
			properties.load(istream);

			parseProperties(properties);

		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
}
