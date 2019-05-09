package com.trovent.streamprocessor.kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class KafkaProducerListener implements UpdateListener {

	private Producer kafkaProducer;

	private Logger logger;

	private String statementName;

	KafkaProducerListener(Producer producer, String statementName) {
		this.kafkaProducer = producer;
		this.logger = LogManager.getLogger();
		this.setStatementName(statementName);
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {

		for (EventBean eb : newEvents) {
			logger.info("{} {} {}  count:{}", eb.get("syslog_timestamp"), eb.get("syslog_app"),
					eb.get("syslog_message"), eb.get("count(*)"));

			this.kafkaProducer.send(String.format("%s %s %s %d", eb.get("syslog_timestamp"), eb.get("syslog_app"),
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
