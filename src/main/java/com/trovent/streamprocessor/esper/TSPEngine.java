package com.trovent.streamprocessor.esper;

import java.util.HashMap;
import java.util.Map;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.UpdateListener;

public class TSPEngine {

	private EPServiceProvider epService;
	HashMap<String, EventType> eventTypes;
	private int statementCounter;
	Map<String, Class<?>> lookupTypeName;

	public TSPEngine() {
		eventTypes = new HashMap<String, EventType>();
		
		lookupTypeName = new HashMap<String, Class<?>>();
		try {
			lookupTypeName.put("string", Class.forName("java.lang.String"));
			lookupTypeName.put("integer", Class.forName("java.lang.Integer"));
			lookupTypeName.put("int", Class.forName("java.lang.Integer"));			
			lookupTypeName.put("boolean", Class.forName("java.lang.Boolean"));
			lookupTypeName.put("long", Class.forName("java.lang.Long"));
			lookupTypeName.put("double", Class.forName("java.lang.Double"));
			lookupTypeName.put("float", Class.forName("java.lang.Float"));
			lookupTypeName.put("byte", Class.forName("java.lang.Byte"));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
		}
		
	}

	/**
	 * Initializes the Service Provider Configures the Engine using the
	 * configuration xml(not created)
	 */
	public void init() {

		/*
		 * this could later be used to startUp with a config file // Configuration
		 * config = new Configuration(); // config.configure("configuration.xml");
		 * 
		 * //using null as URI returns the default Service //different Provider URIs can
		 * be used for other Service Providers //epService =
		 * EPServiceProviderManager.getProvider(null, config);
		 * 
		 */
		epService = EPServiceProviderManager.getDefaultProvider();
	}

	public void shutdown() {
		epService.destroy();
		epService = null;
	}

	/**
	 * Creates a new EsperStatement and an identifying name and assigns a unique id
	 * 
	 * @param statement     the specifications of the new Statement this will be
	 *                      used to select certain types of event
	 * @param statementName a (preferably) unique Name which will later be used to
	 *                      identify the Statement
	 * @return The actual given name of the new Statement. This can diverge from the
	 *         input, as in case of a Statement with this name already existing a
	 *         suffix is appended to preserve uniqueness
	 * @throws EPException when the expression was not valid
	 */
	public String addEPLStatement(String statement, String statementName) throws EPException {
		// creates a new Statement
		EPStatement eplStatement;
		eplStatement = epService.getEPAdministrator().createEPL(statement, statementName);

		this.eventTypes.put(eplStatement.getEventType().getName(), eplStatement.getEventType());

		return eplStatement.getName();
	}

	/**
	 * stops and destroys an EsperStatement via its given Name
	 * 
	 * @param name the unique name of the statement
	 */
	public void removeEPLStatement(String name) {
		EPStatement statement = epService.getEPAdministrator().getStatement(name);
		if (statement != null) {
			statement.destroy();
		}
		else {
			throw new EPException(String.format("there is no statement with the name '%s'",name));
		}
		// TODO remove eventtype from Map
	}

	/**
	 * starts an EsperStatement via its given Name
	 * 
	 * @param name the unique name of the statement
	 */
	public void startEPLStatement(String name) {
		EPStatement statement = epService.getEPAdministrator().getStatement(name);
		if (statement != null) {
			statement.start();
		}
		else {
			throw new EPException(String.format("there is no statement with the name '%s'",name));
		}
	}

	/**
	 * stops an EsperStatement via its given Name
	 * 
	 * @param name the unique name of the statement
	 */
	public void stopEPLStatement(String name) {
		EPStatement statement = epService.getEPAdministrator().getStatement(name);
		if (statement != null) {
			statement.stop();
		}
		else {
			throw new EPException(String.format("there is no statement with the name '%s'",name));
		}
	}

	/**
	 * @author lukas
	 * @param name the name of the Statement a Listener is to be attached to
	 */
	public void addEPLListener(String name) {
		// gets the statement with the corresponding unique Name
		EPStatement statement = epService.getEPAdministrator().getStatement(name);

		if (statement != null) {
			statement.addListener((newData, oldData) -> {
				EventType evType = newData[0].getEventType();
				for (String propName : evType.getPropertyNames()) {
					Object value = newData[0].get(propName);
					System.out.println(String.format("%s : %s", propName, value));
				}
			});
		}
	}
	
	/**
	 * Attaches the given UpdateListener to a statement defined by name
	 * @param statementName name of the statement the listener will attach to
	 * @param listener the Listener that will be attached
	 */
	public void addListener(String statementName, UpdateListener listener) {
		if(epService.getEPAdministrator().getStatement(statementName)!=null) {
			epService.getEPAdministrator().getStatement(statementName).addListener(listener);
		}
		else {
			throw new EPException(String.format("there is no statement with the name '%s'",statementName));
		}
	}

	/**
	 * TODO improve explanation for map
	 * @param name
	 * @param schema Map with format String,String . First String is the name of the parameter, second the name of the class <br>
	 *  currently allowed:
	 */
	public void addEPLSchema(String name, Map<String, String> schema) {
		/*
		 * { "name" : "string", "age" : "integer" }
		 */
		
		// "string"  => String.class
		
		Map<String, Object> ev = new HashMap<String, Object>(); 
		
		
		for ( Map.Entry<String,String> entry : schema.entrySet() ){
			String typeName = entry.getValue();
			Class<?> javaType = lookupTypeName.get(typeName.toLowerCase());
			if (javaType==null) {
				throw new EPException(String.format("can not find type with name '%s'",typeName));
			}
			ev.put( entry.getKey(), javaType);
		}
		
		this.epService.getEPAdministrator().getConfiguration().addEventType(name, ev);			
	}

	/**
	 * TODO standard call to send a given event This function sends a
	 * ObjectArray-type event
	 * 
	 * @param eventTypeName
	 * @param data          the content of your event as an Array of Objects
	 */
	public void sendEPLEvent(String eventTypeName, Object[] data) {
		// Event: { dataA; dataB; dataC; ... }
		// EventType eventType = this.eventTypes.get(eventTypeName);

		epService.getEPRuntime().sendEvent(data, eventTypeName);

	}

	/**
	 * standard call to send a given event This function sends a Map-type event
	 * 
	 * @param eventTypeName
	 * @param data          the content of your event as a Map
	 */
	public void sendEPLEvent(String eventTypeName, Map<?, ?> data) {
		// Event: { dataA; dataB; dataC; ... }
		// EventType eventType = this.eventTypes.get(eventTypeName);

		epService.getEPRuntime().sendEvent(data, eventTypeName);

	}
	
	/**
	 * @return the EPServiceProvider
	 */
	public EPServiceProvider getEPServiceProvider(){
		return epService;
	}
	
	

}
