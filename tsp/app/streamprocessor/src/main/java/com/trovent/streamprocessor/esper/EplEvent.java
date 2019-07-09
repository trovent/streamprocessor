package com.trovent.streamprocessor.esper;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

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
	public void dataFromJson(String jsonString, String source) 
			throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		if (source == null || source.equals("")) {
			this.data = mapper.readValue(jsonString, this.data.getClass());
		} else {
			Map<String, LinkedHashMap<String, Object>> nestedData = mapper.readValue(jsonString, Map.class);
			this.data = nestedData.get(source);
		}
	}

	public String toJson(String destination) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		if (destination == null || destination.equals("")) {
			return mapper.writeValueAsString(this);
		} else {
			HashMap<String, Object> event = new HashMap<>();
			event.put(destination, this.data);
			event.put("eventTypeName", eventTypeName);
			return mapper.writeValueAsString(event);
		}
	}

	public String dataToJson() throws JsonProcessingException {
		ObjectMapper jackson = new ObjectMapper();
		return jackson.writeValueAsString(this.data);
	}

	public String dataToJson(String source) throws JsonProcessingException {
		// return jackson.writeValueAsString(this.data);
		// LinkedHashMap<String, Object> => "{ <String> : <object> }"

		// new: { <source> : { <String> : <object> } }
		HashMap<String, Object> event = new HashMap<>();
		event.put(source, this.data);

		ObjectMapper jackson = new ObjectMapper();
		return jackson.writeValueAsString(event);
	}
}
