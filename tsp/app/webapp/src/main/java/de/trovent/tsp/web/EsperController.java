package de.trovent.tsp.web;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.trovent.streamprocessor.esper.EplEvent;
import com.trovent.streamprocessor.esper.EplSchema;
import com.trovent.streamprocessor.esper.EplStatement;
import com.trovent.streamprocessor.esper.TSPEngine;

import de.trovent.tsp.web.dto.AppStatus;

@RestController
@RequestMapping(value = "esper")
public class EsperController {
	
	private Logger logger = LogManager.getLogger();

	static TSPEngine epService = null;

	public EsperController() {
		if (epService == null) {
			epService = TSPEngine.create();
			epService.init();
		}
	}
	
	static public TSPEngine getEngine() {
		return epService;
	}
	
	@RequestMapping(value="status", method = RequestMethod.GET, headers = "Accept=application/json")
	public AppStatus status() {
		logger.info("Getting status of the Esper Rest Controller");
		return new AppStatus("Esper", "OK");
	}
	
	@RequestMapping(
			value = "statement/{name}", 
			method = RequestMethod.GET, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.OK)
	public EplStatement getEplStatement(@PathVariable("name") String name) {
		if (epService.hasStatement(name)) {
			return epService.getStatement(name);
		}
		return null;
	}
	
	@RequestMapping(
			value = "statement", 
			method = RequestMethod.POST, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.CREATED)
	public String addEplStatement(@RequestBody EplStatement stmt) {
		epService.addEPLStatement(stmt.expression, stmt.name);
		return stmt.name;
	}

	@RequestMapping(
			value = "statement/{name}", 
			method = RequestMethod.DELETE, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.NO_CONTENT)
	public void deleteEplStatement(@PathVariable("name") String name) {
		if (!epService.hasStatement(name)) {
			throw new IllegalArgumentException();
		}
		epService.removeEPLStatement(name);
	}

	@RequestMapping(
			value = "statements", 
			method = RequestMethod.GET, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.OK)
	public List<EplStatement> getEplStatements() {
		return epService.getStatements();
	}

	@RequestMapping(
			value = "schema/{name}", 
			method = RequestMethod.GET, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.OK)
	public EplSchema getEplSchema(@PathVariable("name") String name) {
		if (epService.hasEPLSchema(name)) {
			return epService.getEPLSchema(name);
		}
		return null;
	}

	@RequestMapping(
			value = "schema", 
			method = RequestMethod.POST, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.CREATED)
	public String addEplSchema(@RequestBody EplSchema schema) {
		epService.addEPLSchema(schema);
		return schema.name;
	}

	@RequestMapping(
			value = "schema/{name}", 
			method = RequestMethod.DELETE, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.NO_CONTENT)
	public void deleteEplSchema(@PathVariable("name") String name) {
		if (!epService.hasEPLSchema(name)) {
			throw new IllegalArgumentException();
		}
		epService.removeEPLSchema(name);
	}

	@RequestMapping(
			value = "schemas", 
			method = RequestMethod.GET, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.OK)
	public List<EplSchema> getEplSchemas() {
		return epService.getEPLSchemas();
	}

	@RequestMapping(
			value = "sendEvent/map", 
			method = RequestMethod.POST, 
			headers = "Accept=application/json")
	@ResponseStatus(value=HttpStatus.OK)
	public void sendEvent(@RequestBody EplEvent event) {
		epService.sendEPLEvent(event);
	}

}

