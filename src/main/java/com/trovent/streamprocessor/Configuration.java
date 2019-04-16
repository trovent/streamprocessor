package com.trovent.streamprocessor;

import org.apache.logging.log4j.LogManager;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class Configuration {
	
	private CmdLineParser parser;
	
	@Option(name="-c", usage="Sets config file name")
	public String configfile;
	
	public String getConfigfile() {
		return configfile;
	}

	public Configuration() {
		
		parser = new CmdLineParser(this);			
		
	}
	
	public void parse(String[] args) {
		
		try {
			parser.parseArgument(args);
					
		} catch (CmdLineException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			
			LogManager.getLogger().error(e.getMessage());
		}
		
	}
}
