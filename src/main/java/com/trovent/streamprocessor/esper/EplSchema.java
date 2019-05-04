package com.trovent.streamprocessor.esper;

import java.util.HashMap;
import java.util.Map;

public class EplSchema {
	public String name;
	public Map<String, String> fields;

	public EplSchema() {
		fields = new HashMap<String, String>();
	}

	public EplSchema(String name) {
		this.name = name;
		fields = new HashMap<String, String>();
	}

	public EplSchema add(String key, String value) {
		this.fields.put(key, value);
		return this;
	}
}
