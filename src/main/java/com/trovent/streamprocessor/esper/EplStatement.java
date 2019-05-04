package com.trovent.streamprocessor.esper;

public class EplStatement {
	public String name;
	public String expression;

	public EplStatement() {
	}

	public EplStatement(String name) {
		this.name = name;
	}

	public EplStatement(String name, String expression) {
		this.name = name;
		this.expression = expression;
	}

}
