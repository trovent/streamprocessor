package com.trovent.streamprocessor;

import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class Application {

	
	@Option(name="-c", usage="Sets config file name")
	public String configfile;
	
	/**
	 * Initialise application
	 * @param args
	 */	
	private void init(String[] args) {
		System.out.println("Application.init()");

		// TODO
		/*
		 * parse commandline parameters
		 * open configfile, read settings
		 * initialise logging component
		 *  
		 */
		CmdLineParser parser = new CmdLineParser(this);
		try {
			parser.parseArgument(args);
			
			if (this.configfile!=null) {
				System.out.println(this.configfile);	
			} else {
				System.out.println("Option -c is not set");
			}
			
		} catch (CmdLineException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

				
		
		System.out.println("Application.init() done");		
	}
	
	
	private void run() {
		System.out.println("Application.run()");
		// TODO
		/*
		 * init and start esper engine
		 * test connection to kafka
		 * start application server
		 */
		System.out.println("Application.run() done");
	}

	public static void main(String[] args) {		
		Application app = new Application();
		app.init(args);
		app.run();
	}

}
