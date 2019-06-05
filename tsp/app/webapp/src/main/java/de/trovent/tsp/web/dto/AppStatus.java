package de.trovent.tsp.web.dto;

public class AppStatus {
	
	private String component;
	private String status;

	public AppStatus(String component, String status) {
		super();
		this.component = component;
		this.status = status;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getComponent() {
		return component;
	}

	public void setComponent(String component) {
		this.component = component;
	}
	
	

}
