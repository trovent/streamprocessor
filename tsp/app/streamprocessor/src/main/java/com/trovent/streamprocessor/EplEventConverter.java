package com.trovent.streamprocessor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;

import java.util.Set;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

import com.espertech.esper.client.EventPropertyDescriptor;
import com.espertech.esper.client.EventType;
import com.trovent.streamprocessor.esper.EplEvent;

public class EplEventConverter {

	private EventType eventType;

	private HashMap<String, Class<?>> typeLookupTable;

	private final static Set<Class<?>> convertibleTypes = Collections
			.unmodifiableSet(new HashSet<Class<?>>(Arrays.asList(ZonedDateTime.class, LocalDateTime.class,
					LocalDate.class, LocalTime.class, OffsetDateTime.class, OffsetTime.class, Duration.class)));

	/**
	 * Constructor of EplEventConverter Create lookup table with names of the fields
	 * to be converted, based on the given event type
	 * 
	 * @param eventType Esper event type describing all field names and types
	 */
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

	/**
	 * For all types of the event found in the lookup table, exec a conversion.
	 * Possible conversions: (string) => { LocalDateTime, ZonedDateTime, LocalData,
	 * LocalTime, OffsetDateTime, OffsetTime, Duration } (integer) => { LocalDate,
	 * LocalTime, Duration }
	 * 
	 * @param event EplEvent containing data fields that have to be converted.
	 */
	public void convertTypes(EplEvent event) {

		//
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

	/**
	 * Convert value from a given string into a complex object given by class type.
	 * 
	 * @param value        string value to be converted
	 * @param expectedType type of the class to be returned
	 * @return converted value, can be null on failed conversion
	 */
	private Object convert(String value, Class<?> expectedType) {
		try {
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
		}
		catch (DateTimeParseException e) {
			LogManager.getLogger().warn("Exception on conversion: {} ({})", e.getMessage(), e.getParsedString());
		}
		return null;
	}

	/**
	 * Convert value from a given integer into a complex object given by class type.
	 * 
	 * @param value        integer value to be converted
	 * @param expectedType type of the class to be returned
	 * @return converted value, can be null on failed conversion
	 */
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
