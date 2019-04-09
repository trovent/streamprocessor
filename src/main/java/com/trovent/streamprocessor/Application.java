package com.trovent.streamprocessor;

public class Application {

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
		
		Configuration config = new Configuration();
		
		config.parse(args);
				
		if (config.getConfigfile()!=null) {
			System.out.println(config.getConfigfile());	
		} else {
			System.out.println("Option -c is not set");
		}
		
		System.out.println("Application.init() done\n");		
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
