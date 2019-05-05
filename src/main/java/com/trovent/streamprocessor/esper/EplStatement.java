package com.trovent.streamprocessor.esper;

/**
 * Entity class for epl statement data
 * 
 * @author tobias nieberg
 *
 */
public class EplStatement {
	public String name;
	public String expression;

	/**
	 * Default constructor of EplStatement
	 */
	public EplStatement() {
	}

	/**
	 * Extended constructor of EplStatement. The given name is set as name of the
	 * EplStatement.
	 * 
	 * @param name name to be used as name of EplStatement
	 */
	public EplStatement(String name) {
		this.name = name;
	}

	/**
	 * Extended constructor of EplStatement. Given name and expression are used as
	 * name and expression of the EplStatement.
	 * 
	 * @param name       name to be used as name of EplStatement
	 * @param expression expression to be used as expression of EplStatement
	 */
	public EplStatement(String name, String expression) {
		this.name = name;
		this.expression = expression;
	}

}
