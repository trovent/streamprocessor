package com.trovent.streamprocessor.esper;

import java.util.LinkedHashMap;

public class EplEvent {
	public String eventTypeName;
	public LinkedHashMap<String, Object> data;

	public EplEvent() {
		this.data = new LinkedHashMap<String, Object>();
	}

	public EplEvent(String eventTypeName) {
		this.data = new LinkedHashMap<String, Object>();
		this.eventTypeName = eventTypeName;
	}

	public EplEvent add(String fieldName, Object data) {
		this.data.put(fieldName, data);
		return this;
	}
}
