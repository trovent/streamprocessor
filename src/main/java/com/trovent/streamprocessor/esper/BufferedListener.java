/**
 * 
 */
package com.trovent.streamprocessor.esper;

import java.util.LinkedList;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.espertech.esper.client.UpdateListener;

/**
 * @author tobias nieberg
 *
 *         This class provides an event listener that puts all incoming events
 *         into a queue. This queue can read read via the poll() method.
 */
public class BufferedListener implements UpdateListener {

	private LinkedList<EplEvent> queue = new LinkedList<>();

	// Return one line of data and remove it from the queue
	public EplEvent poll() {
		return this.queue.poll();
	}

	public EplEvent peek() {
		return this.queue.peek();
	}

	public int size() {
		return this.queue.size();
	}

	public boolean isEmpty() {
		return this.queue.isEmpty();
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		// TODO Auto-generated method stub
		for (EventBean bean : newEvents) {
			// put data into buffer

			EplEvent event = new EplEvent(bean.getEventType().getName());
			for (EventPropertyDescriptor descriptor : bean.getEventType().getPropertyDescriptors()) {
				String propertyName = descriptor.getPropertyName();
				Object propertyValue = bean.get(propertyName);
				event.add(propertyName, propertyValue);
			}
			this.queue.add(event);
		}
	}

}
