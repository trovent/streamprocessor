package com.trovent.streamprocessor.esper;

/**
 * Entity class for epl schema data
 * 
 * @author tobias nieberg
 *
 */
import java.util.HashMap;
import java.util.Map;

public class EplSchema {
	public String name;
	public Map<String, String> fields;

	/**
	 * Default constructor creating empty object
	 */
	public EplSchema() {
		fields = new HashMap<String, String>();
	}

	/**
	 * Extended constructor. Use given name as schema name.
	 * 
	 * @param name name to be set for schema
	 */
	public EplSchema(String name) {
		this.name = name;
		fields = new HashMap<String, String>();
	}

	/**
	 * Add field given as key value pair to schema.
	 * 
	 * @param fieldName name of the field to be defined
	 * @param fieldType type of the field given as string
	 * @return The current EplSchema object is returned. Can be used to create a
	 *         call chain of the form add().add().add()...
	 */
	public EplSchema add(String fieldName, String fieldType) {
		this.fields.put(fieldName, fieldType);
		return this;
	}
}
