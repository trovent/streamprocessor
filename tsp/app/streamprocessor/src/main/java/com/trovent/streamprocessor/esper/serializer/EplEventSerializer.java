package com.trovent.streamprocessor.esper.serializer;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.trovent.streamprocessor.esper.EplEvent;

public class EplEventSerializer extends StdSerializer<EplEvent> {
    
	private static final long serialVersionUID = 1L;
	
	private String destination;

	public EplEventSerializer() {
        this(null);
    }
   
    public EplEventSerializer(Class<EplEvent> t) {
        super(t);
    }
 
    @Override
    public void serialize(EplEvent value, JsonGenerator jgen, SerializerProvider provider) 
      throws IOException, JsonProcessingException {
  
        jgen.writeStartObject();
        jgen.writeStringField("eventTypeName", value.eventTypeName);
        jgen.writeObjectField(this.destination, value.data);
        jgen.writeEndObject();
    }

	public void setDestination(String destination) {
		this.destination = destination;
	}
}
