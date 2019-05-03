package com.trovent.streamprocessor.esper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.espertech.esper.client.ConfigurationException;
import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.UpdateListener;

public class TSPEngine {

	private EPServiceProvider epService;
	HashMap<String, EventType> eventTypes;
	Map<String, Class<?>> lookupTypeName;

	public TSPEngine() {
		eventTypes = new HashMap<String, EventType>();

		lookupTypeName = new HashMap<String, Class<?>>();
		lookupTypeName.put("string", String.class);
		lookupTypeName.put("integer", int.class);
		lookupTypeName.put("int", int.class);
		lookupTypeName.put("boolean", boolean.class);
		lookupTypeName.put("long", long.class);
		lookupTypeName.put("double", double.class);
		lookupTypeName.put("float", float.class);
		lookupTypeName.put("byte", byte.class);
		lookupTypeName.put("biginteger", BigInteger.class);
		lookupTypeName.put("bigdecimal", BigDecimal.class);
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
		} else {
			throw new EPException(String.format("there is no statement with the name '%s'", name));
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
		} else {
			throw new EPException(String.format("there is no statement with the name '%s'", name));
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
		} else {
			throw new EPException(String.format("there is no statement with the name '%s'", name));
		}
	}

	/**
	 * returns an array of the names of all statements
	 * 
	 * @return
	 */
	public String[] getStatementNames() {
		return epService.getEPAdministrator().getStatementNames();
	}

	/**
	 * returns a Map, with the statement names as key and the expression as content
	 * 
	 * @return
	 */
	public Map<String, String> getStatements() {
		String[] statementNames = getStatementNames();
		Map<String, String> statements = new LinkedHashMap<String, String>();

		for (String s : statementNames) {
			statements.put(s, epService.getEPAdministrator().getStatement(s).getText());
		}

		return statements;
	}

	/**
	 * returns the content of a statement.
	 * 
	 * @param statementName
	 * @return
	 */
	public String getStatementExpression(String statementName) {
		return epService.getEPAdministrator().getStatement(statementName).getText();
	}

	/**
	 * Looks into Configuration and checks if Schema with the given Name exists
	 * 
	 * @param eventTypeName Name of the Schema
	 * @return
	 */
	public boolean hasSchema(String eventTypeName) {
		return epService.getEPAdministrator().getConfiguration().isEventTypeExists(eventTypeName);
	}

	/**
	 * Checks if Schema with the given Name exists
	 * 
	 * @param eventTypeName Name of the Schema
	 * @return
	 */
	public boolean hasStatement(String statementName) {
		return (epService.getEPAdministrator().getStatement(statementName) != null);
	}

	/**
	 * Attaches the given UpdateListener to a statement defined by name
	 * 
	 * @param statementName name of the statement the listener will attach to
	 * @param listener      the Listener that will be attached
	 */
	public void addListener(String statementName, UpdateListener listener) {
		if (epService.getEPAdministrator().getStatement(statementName) != null) {
			epService.getEPAdministrator().getStatement(statementName).addListener(listener);
		} else {
			throw new EPException(String.format("there is no statement with the name '%s'", statementName));
		}
	}

	/**
	 * TODO improve explanation for map
	 * 
	 * @param name
	 * @param schema Map with format String,String . First String is the name of the
	 *               parameter, second the name of the class <br>
	 *               currently allowed:
	 */
	public void addEPLSchema(String name, Map<String, String> schema) {
		/*
		 * { "name" : "string", "age" : "integer" }
		 */

		// "string" => String.class

		Map<String, Object> ev = new HashMap<String, Object>();

		for (Map.Entry<String, String> entry : schema.entrySet()) {
			String typeName = entry.getValue();
			Class<?> javaType = lookupTypeName.get(typeName.toLowerCase());
			if (javaType == null) {
				throw new EPException(String.format("can not find type with name '%s'", typeName));
			}
			ev.put(entry.getKey(), javaType);
		}

		this.epService.getEPAdministrator().getConfiguration().addEventType(name, ev);
	}

	public void addEPLSchema(String name, String[] propNames, String[] propTypeNames) throws EPException {
		/*
		 * propNames: [ "name", "age ] propTypeNames : [ "string", "integer" ]
		 */

		if (propNames.length != propTypeNames.length) {
			throw new EPException(String.format("number of property names and type names do not match (%d!=%d)",
					propNames.length, propTypeNames.length));
		}
		Object[] propTypes = new Object[propTypeNames.length];

		for (int pos = 0; pos < propNames.length; pos++) {
			Class<?> javaType = lookupTypeName.get(propTypeNames[pos].toLowerCase());
			if (javaType == null) {
				throw new EPException(String.format("can not find type with name '%s'", propTypeNames[pos]));
			}
			propTypes[pos] = javaType;
		}

		this.epService.getEPAdministrator().getConfiguration().addEventType(name, propNames, propTypes);
	}

	/**
	 * removes a Schema with the given Name
	 * 
	 * @param eventTypeName Name of the Statement
	 */
	public void removeEPLSchema(String eventTypeName) throws ConfigurationException {
		if (epService.getEPAdministrator().getConfiguration().isEventTypeExists(eventTypeName)) {
			epService.getEPAdministrator().getConfiguration().removeEventType(eventTypeName, false);
		} else {
			throw new EPException(String.format("EventType with the Name '%s' does not exist", eventTypeName));
		}
	}

	/**
	 * removes a Schema with the given Name <br>
	 * gives the option to force the removal
	 * 
	 * @param name  Name of the Statement
	 * @param force if true, removes a Schema even if there are Statements depending
	 *              on it.
	 */
	public void removeEPLSchema(String eventTypeName, boolean force) throws ConfigurationException {
		if (epService.getEPAdministrator().getConfiguration().isEventTypeExists(eventTypeName)) {
			epService.getEPAdministrator().getConfiguration().removeEventType(eventTypeName, force);
		} else {
			throw new EPException(String.format("EventType with the Name '%s' does not exist", eventTypeName));
		}
	}

	public EventType getEventType(String eventTypeName) {
		EventType lookupType = epService.getEPAdministrator().getConfiguration().getEventType(eventTypeName);
		if (lookupType != null) {
			return lookupType;
		} else {
			throw new EPException(String.format("EventType with the Name '%s' does not exist", eventTypeName));
		}

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
	public EPServiceProvider getEPServiceProvider() {
		return epService;
	}

}
