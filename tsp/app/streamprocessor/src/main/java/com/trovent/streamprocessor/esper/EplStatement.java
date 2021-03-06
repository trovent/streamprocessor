package com.trovent.streamprocessor.esper;

import com.espertech.esper.client.EPStatement;

import io.swagger.annotations.ApiModelProperty;

/**
 * Entity class for epl statement data
 * 
 * @author tobias nieberg
 *
 */
public class EplStatement {

	@ApiModelProperty(notes = "Name of the esper statement", example = "ntp_filter")
	public String name;

	@ApiModelProperty(notes = "Expression of the esper statement", example = "select * from netflow where dest_port=123")
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

	public EplStatement(EPStatement statement) {
		this.name = statement.getName();
		this.expression = statement.getText();
	}

}
