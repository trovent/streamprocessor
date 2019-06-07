package com.trovent.streamprocessor.esper;

import java.io.IOException;
import java.util.LinkedHashMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.swagger.annotations.ApiModelProperty;

public class EplEvent {

	@ApiModelProperty(notes = "Name of the schema/event type", example = "syslog")
	public String eventTypeName;

	@ApiModelProperty(notes = "Data of the event given as key value map", example = " { \"appname\" : \"nginx\", \"hostname\" : \"web.example.com\", \"pid\" : 723, \"message\" : \"Request from host 1.2.3.4 GET /index.html\" } ")
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

	static public EplEvent fromJson(String jsonString) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper jackson = new ObjectMapper();
		return jackson.readValue(jsonString, EplEvent.class);
	}

	@SuppressWarnings("unchecked")
	public void dataFromJson(String jsonString) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper jackson = new ObjectMapper();
		this.data = jackson.readValue(jsonString, this.data.getClass());
	}

	public String toJson() throws JsonProcessingException {
		ObjectMapper jackson = new ObjectMapper();
		return jackson.writeValueAsString(this);
	}

	public String dataToJson() throws JsonProcessingException {
		ObjectMapper jackson = new ObjectMapper();
		return jackson.writeValueAsString(this.data);
	}
}
