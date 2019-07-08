package com.trovent.streamprocessor;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.trovent.streamprocessor.esper.TSPEngine;

/**
 * This class process an event given as a string with comma separated values. It
 * parses the input and passes it to the given event type of esper engine.
 * 
 */
public class CSVInputProcessor extends AbstractInputProcessor {

	/**
	 * Constructor of CSVInputProcessor
	 * 
	 * @param engine Sets the engine to use for event processing
	 */
	public CSVInputProcessor(TSPEngine engine) {
		super(engine);
	}

	/**
	 * Constructor of CSVInputProcessor
	 * 
	 * @param engine Sets the engine to use for event processing
	 * @param eventTypeName name of the event type
	 */
	public CSVInputProcessor(TSPEngine engine, String eventTypeName) throws EPException {
		super(engine, eventTypeName);
	}
	
	/**
	 * Constructor of CSVInputProcessor
	 * 
	 * @param engine Sets the engine to use for event processing
	 * @param eventTypeName name of the event type
	 * @param source key where data is located
	 */
	public CSVInputProcessor(TSPEngine engine, String eventTypeName, String source) throws EPException {
		super(engine, eventTypeName, source);
	}

	/**
	 * Process the incoming data given as string array. If the number of values
	 * matches the number of fields of the previously set event type, the data is
	 * transformed into an esper event and sent to the engine.
	 * 
	 * @param input The input event given json string e.g. '
	 *              "Trovent";2019;"192.168.0.66" '
	 * @return true if the event was processed successfully, false otherwise
	 */
	public Boolean process(String[] input) {
		EventPropertyDescriptor[] descriptors = this.eventType.getPropertyDescriptors();
		if (descriptors.length > input.length) {
			logger.warn("cannot process '{}' - not enough data fields ({} needed)", input, descriptors.length);
			return false;
		}

		Object[] values = new Object[input.length];
		int pos = 0;
		for (EventPropertyDescriptor descriptor : descriptors) {
			String propName = descriptor.getPropertyName();
			Class<?> propType = descriptor.getPropertyType();

			String value = input[pos];
			try {
				if (propType == String.class) {
					values[pos] = value;
				} else if (propType == Integer.class) {
					values[pos] = new Integer(value);
				} else if (propType == Boolean.class) {
					values[pos] = new Boolean(value);
				} else if (propType == Float.class) {
					values[pos] = new Float(value);
				} else if (propType == Double.class) {
					values[pos] = new Double(value);
				} else if (propType == Long.class) {
					values[pos] = new Long(value);
				} else if (propType == Byte.class) {
					values[pos] = new Byte(value);
				} else if (propType == BigInteger.class) {
					values[pos] = new BigInteger(value);
				} else if (propType == BigDecimal.class) {
					values[pos] = new BigDecimal(value);
				}
			} catch (NumberFormatException e) {
				logger.warn("type mismatch for value '{}' of field '{}' - could not convert to {}", value, propName,
						propType.toString());
				return false;
			}

			++pos;
		}
		this.engine.sendEPLEvent(this.eventType.getName(), values);
		return true;
	}

	/**
	 * Process the incoming data given as csv string.
	 * <p>
	 * The csv string is parsed and transformed into an array of strings. If the
	 * number of values matches the number of fields of the previously set event
	 * type, the data is transformed into an esper event and sent to the engine.
	 * 
	 * @param input The input event given json string e.g.
	 *              <code> "Trovent";2019;"192.168.0.66" </code>
	 * @return true if the event was processed successfully, false otherwise
	 */
	@Override
	public Boolean process(String input) {

		this.logger.debug(String.format("input: '%s'", input));

		String[] inputAsArray = input.split(";");

		return this.process(inputAsArray);
	}
}
