package de.trovent.tsp.web.exceptionHandling.dto;

public class CustomError {
	
	private String httpStatus;
	private String reason;
	
	public CustomError(String httpStatus, String reason) {
		super();
		this.httpStatus = httpStatus;
		this.reason = reason;
	}

	public String getHttpStatus() {
		return httpStatus;
	}
	
	public void setHttpStatus(String httpStatus) {
		this.httpStatus = httpStatus;
	}
	
	public String getReason() {
		return reason;
	}
	
	public void setReason(String reason) {
		this.reason = reason;
	}
	
}
