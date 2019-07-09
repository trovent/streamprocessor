package com.trovent.streamprocessor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import com.espertech.esper.client.EventPropertyDescriptor;
import com.espertech.esper.client.EventType;
import com.trovent.streamprocessor.esper.EplEvent;

public class EplEventConverter {

	private EventType eventType;

	private HashMap<String, Class<?>> typeLookupTable;

	private final static Set<Class<?>> convertibleTypes = Collections
			.unmodifiableSet(new HashSet(Arrays.asList(ZonedDateTime.class, LocalDateTime.class, LocalDate.class,
					LocalTime.class, OffsetDateTime.class, OffsetTime.class, Duration.class)));

	EplEventConverter(EventType eventType) {
		this.eventType = eventType;
		this.typeLookupTable = new HashMap<>();
		
		for (EventPropertyDescriptor propDesc : this.eventType.getPropertyDescriptors()) {
			Class<?> propType = (propDesc.getPropertyType());
			if (convertibleTypes.contains(propType)) {
				// if this field contains a "convertible type", then add it to the lookup table
				this.typeLookupTable.put(propDesc.getPropertyName(), propDesc.getPropertyType());
			}
		}
	}

	public void convertTypes(EplEvent event) {

		// for all types of the event found in the lookup table, exec a conversion
		// (string => non-primitive type)
		for (Entry<String, Class<?>> entry : this.typeLookupTable.entrySet()) {
			String fieldName = entry.getKey();
			Class<?> expectedType = entry.getValue();
			Object value = event.data.get(fieldName);
			if (value instanceof String) {
				event.data.put(fieldName, this.convert((String) value, expectedType));
			}
			else if (value instanceof Integer) {
				event.data.put(fieldName, this.convert((Integer) value, expectedType));
			}
		}
	}

	private Object convert(String value, Class<?> expectedType) {
		if (expectedType == LocalDateTime.class) {
			return LocalDateTime.parse(value);
		}
		if (expectedType == ZonedDateTime.class) {
			return ZonedDateTime.parse(value);
		}
		if (expectedType == LocalDate.class) {
			return LocalDate.parse(value);
		}
		if (expectedType == LocalTime.class) {
			return LocalTime.parse(value);
		}
		if (expectedType == OffsetDateTime.class) {
			return OffsetDateTime.parse(value);
		}
		if (expectedType == OffsetTime.class) {
			return OffsetTime.parse(value);
		}
		if (expectedType == Duration.class) {
			return Duration.parse(value);
		}
		
		return null;
	}

	private Object convert(Integer value, Class<?> expectedType) {

		if (expectedType == LocalDate.class) {
			return LocalDate.ofEpochDay(value);
		}
		if (expectedType == LocalTime.class) {
			return LocalTime.ofNanoOfDay(value);
		}
		if (expectedType == Duration.class) {
			return Duration.ofMillis(value);
		}
		
		return null;
	}
}
