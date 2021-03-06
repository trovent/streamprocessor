package com.trovent.streamprocessor.esper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.time.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.espertech.esper.client.ConfigurationException;
import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.UpdateListener;

public class TSPEngine {

	private EPServiceProvider epService;
	Map<String, Class<?>> lookupTypeName;

	private static TSPEngine engine = null;

	private Logger logger = null;

	private TSPEngine() {
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

		lookupTypeName.put("duration", Duration.class);
		lookupTypeName.put("localdate", LocalDate.class);
		lookupTypeName.put("localdatetime", LocalDateTime.class);
		lookupTypeName.put("localtime", LocalTime.class);
		lookupTypeName.put("offsetdatetime", OffsetDateTime.class);
		lookupTypeName.put("offsettime", OffsetTime.class);
		lookupTypeName.put("zoneddatetime", ZonedDateTime.class);

		logger = LogManager.getLogger();
	}

	/**
	 * returns the (unique) TSPEngine <br>
	 * creates a new one if it currently does not exist
	 * 
	 * @return
	 */
	public static TSPEngine create() {
		if (engine == null) {
			engine = new TSPEngine();
		}
		return engine;
	}

	/**
	 * Initializes the Service Provider Configures the Engine using the
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
	 * Creates a new EsperStatement and an identifying name
	 * 
	 * @param statement     the specifications of the new Statement this will be
	 *                      used to select certain types of event
	 * @param statementName a (preferably) unique Name which will later be used to
	 *                      identify the Statement
	 * @return The actual given name of the new Statement. This can diverge from the
	 *         input, as in case of a Statement with an already existing name a
	 *         suffix is appended to preserve uniqueness
	 * @throws EPException when the expression was not valid
	 */
	public String addEPLStatement(String expression, String statementName) throws EPException {

		this.logger.info("add statement: {}", statementName);
		this.logger.debug("statement expression: {}", expression);

		EPStatement eplStatement;
		eplStatement = epService.getEPAdministrator().createEPL(expression, statementName);

		return eplStatement.getName();
	}

	/**
	 * Creates a new EsperStatement and an identifying name
	 * 
	 * @param statement a EplStatement object which will be used to select certain
	 *                  types of event
	 * @return The actual given name of the new Statement. This can diverge from the
	 *         input, as in case of a Statement with an already existing name a
	 *         suffix is appended to preserve uniqueness
	 * @throws EPException when the expression was not valid
	 */
	public String addEPLStatement(EplStatement statement) throws EPException {

		return this.addEPLStatement(statement.expression, statement.name);
	}

	/**
	 * stops and destroys an EsperStatement via its given Name
	 * 
	 * @param statementName the unique name of the statement
	 */
	public void removeEPLStatement(String statementName) {

		this.logger.info("remove statement: {}", statementName);
		EPStatement statement = epService.getEPAdministrator().getStatement(statementName);
		if (statement != null) {
			statement.destroy();
		} else {
			throw new EPException(String.format("there is no statement with the name '%s'", statementName));
		}
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
	 * Returns a List of EplStatements
	 * 
	 * @return
	 */
	public List<EplStatement> getStatements() {
		List<EplStatement> statements = new ArrayList<EplStatement>();
		for (String sName : epService.getEPAdministrator().getStatementNames()) {
			statements.add(new EplStatement(epService.getEPAdministrator().getStatement(sName)));
		}
		return statements;
	}

	public EplStatement getStatement(String statementName) {
		EplStatement stmnt = new EplStatement(epService.getEPAdministrator().getStatement(statementName));
		return stmnt;

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
	public boolean hasEPLSchema(String eventTypeName) {
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
	 * Removes the given UpdateListener from a statement defined by name
	 * 
	 * @param statementName name of the statement the listener will attach to
	 * @param listener      the Listener that will be removed
	 */
	public void removeListener(String statementName, UpdateListener listener) {
		if (epService.getEPAdministrator().getStatement(statementName) != null) {
			epService.getEPAdministrator().getStatement(statementName).removeListener(listener);
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
	 * @throws EPException
	 */
	public void addEPLSchema(String name, Map<String, String> schema) throws EPException {
		/*
		 * { "name" : "string", "age" : "integer" }
		 */

		// "string" => String.class

		this.logger.info("adding schema: {}", name);
		this.logger.debug("schema definition: {}", schema);

		Map<String, Object> ev = new HashMap<String, Object>();

		try {
			for (Map.Entry<String, String> entry : schema.entrySet()) {
				String typeName = entry.getValue();
				Class<?> javaType = lookupTypeName.get(typeName.toLowerCase());
				if (javaType == null) {
					javaType = Class.forName(typeName);
				}

				ev.put(entry.getKey(), javaType);
			}
			this.epService.getEPAdministrator().getConfiguration().addEventType(name, ev);

		} catch (ClassNotFoundException e) {

			throw new EPException(String.format("can not find type (%s)", e.getMessage()));
		}
	}

	/**
	 * Adds a new Schema using two Arrays to identify properties
	 * 
	 * @param name
	 * @param propNames
	 * @param propTypeNames
	 * @throws EPException
	 */
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
	 * Adds a new Schema using a EplSchema object
	 * 
	 * @param schema
	 */
	public void addEPLSchema(EplSchema schema) {
		this.addEPLSchema(schema.name, schema.fields);
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

		this.logger.info("remove schema: {}", eventTypeName);
		if (epService.getEPAdministrator().getConfiguration().isEventTypeExists(eventTypeName)) {
			epService.getEPAdministrator().getConfiguration().removeEventType(eventTypeName, force);
		} else {
			throw new EPException(String.format("EventType with the Name '%s' does not exist", eventTypeName));
		}
	}

	/**
	 * Returns the EventType object corresponding to the given name
	 * 
	 * @param eventTypeName
	 * @return
	 */
	public EplSchema getEPLSchema(String eventTypeName) {
		EventType lookupType = epService.getEPAdministrator().getConfiguration().getEventType(eventTypeName);
		if (lookupType != null) {
			return new EplSchema(lookupType);
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
	 * Returns a List of all currently available EventTypes/Schemas
	 * 
	 * @return
	 */
	public List<EplSchema> getEPLSchemas() {
		EventType[] all = epService.getEPAdministrator().getConfiguration().getEventTypes();
		List<EplSchema> schemas = new ArrayList<EplSchema>();

		for (EventType e : all) {
			schemas.add(new EplSchema(e));
		}

		return schemas;
	}

	/**
	 * TODO standard call to send a given event This function sends a
	 * ObjectArray-type event
	 * 
	 * @param eventTypeName
	 * @param data          the content of your event as an Array of Objects
	 */
	public void sendEPLEvent(String eventTypeName, Object[] data) throws EPException {
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
	public void sendEPLEvent(String eventTypeName, Map<String, Object> data) {
		// Event: { dataA; dataB; dataC; ... }
		// EventType eventType = this.eventTypes.get(eventTypeName);

		epService.getEPRuntime().sendEvent(data, eventTypeName);

	}

	/**
	 * standard call to send a given event This function sends a Map-type event
	 * 
	 * @param event The EplEvent to be send to the Runtime Engine <br>
	 *              The event EplEvent class contains both the event name as well as
	 *              the data
	 */
	public void sendEPLEvent(EplEvent event) {
		epService.getEPRuntime().sendEvent(event.data, event.eventTypeName);
	}

	/**
	 * @return the EPServiceProvider
	 */
	public EPServiceProvider getEPServiceProvider() {
		return epService;
	}

}
