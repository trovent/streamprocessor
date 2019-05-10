package com.trovent.streamprocessor.kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class ProducerListener implements UpdateListener {

	private IProducer producer;

	private Logger logger;

	private String statementName;

	ProducerListener(IProducer producer, String statementName) {
		this.producer = producer;
		this.logger = LogManager.getLogger();
		this.setStatementName(statementName);
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {

		for (EventBean eb : newEvents) {
			logger.info("{} {} {}  count:{}", eb.get("syslog_timestamp"), eb.get("syslog_app"),
					eb.get("syslog_message"), eb.get("count(*)"));

			this.producer.send(String.format("%s %s %s %d", eb.get("syslog_timestamp"), eb.get("syslog_app"),
					eb.get("syslog_message"), eb.get("count(*)")));
		}

	}

	public String getStatementName() {
		return statementName;
	}

	public void setStatementName(String statementName) {
		this.statementName = statementName;
	}

}
