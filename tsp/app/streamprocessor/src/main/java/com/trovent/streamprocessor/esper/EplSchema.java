package com.trovent.streamprocessor.esper;

import java.util.LinkedHashMap;

import com.espertech.esper.client.EventType;

import io.swagger.annotations.ApiModelProperty;

public class EplSchema {

	@ApiModelProperty(notes = "Name of the esper schema", example = "syslog")
	public String name;

	@ApiModelProperty(notes = "Definition of the esper schema", example = "{ " + "\"hostname\" : \"string\", "
			+ "\"appname\" : \"string\", " + "\"pid\" : \"integer\", " + "\"message\" : \"string\" " + "}")
	public LinkedHashMap<String, String> fields;

	/**
	 * Default constructor creating empty object
	 */
	public EplSchema() {
		fields = new LinkedHashMap<String, String>();
	}

	/**
	 * Extended constructor. Use given name as schema name.
	 * 
	 * @param name name to be set for schema
	 */
	public EplSchema(String name) {
		this.name = name;
		fields = new LinkedHashMap<String, String>();
	}

	public EplSchema(EventType eventType) {
		this.name = eventType.getName();
		fields = new LinkedHashMap<String, String>();
		for (String propName : eventType.getPropertyNames()) {
			fields.put(propName, eventType.getPropertyDescriptor(propName).getPropertyType().getSimpleName());
		}
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
