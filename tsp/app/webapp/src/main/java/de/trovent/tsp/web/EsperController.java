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
import io.swagger.annotations.ApiOperation;

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

	@ApiOperation(value = "Get status of EsperController component")
	@RequestMapping(value = "status", method = RequestMethod.GET, headers = "Accept=application/json")
	public AppStatus status() {
		logger.info("Getting status of the Esper Rest Controller");
		return new AppStatus("Esper", "OK");
	}

	@ApiOperation(value = "Get esper statement identified by name")
	@RequestMapping(value = "statement/{name}", method = RequestMethod.GET, headers = "Accept=application/json")
	@ResponseStatus(value = HttpStatus.OK)
	public EplStatement getEplStatement(@PathVariable("name") String name) {
		if (epService.hasStatement(name)) {
			return epService.getStatement(name);
		}
		return null;
	}

	@ApiOperation(value = "Add new esper statement")
	@RequestMapping(value = "statement", method = RequestMethod.POST, headers = "Accept=application/json")
	@ResponseStatus(value = HttpStatus.CREATED)
	public String addEplStatement(@RequestBody EplStatement stmt) {
		epService.addEPLStatement(stmt.expression, stmt.name);
		return stmt.name;
	}

	@ApiOperation(value = "Delete esper statement identified by name")
	@RequestMapping(value = "statement/{name}", method = RequestMethod.DELETE, headers = "Accept=application/json")
	@ResponseStatus(value = HttpStatus.NO_CONTENT)
	public void deleteEplStatement(@PathVariable("name") String name) {
		if (!epService.hasStatement(name)) {
			throw new IllegalArgumentException();
		}
		epService.removeEPLStatement(name);
	}

	@ApiOperation(value = "Get all defined esper statements")
	@RequestMapping(value = "statements", method = RequestMethod.GET, headers = "Accept=application/json")
	@ResponseStatus(value = HttpStatus.OK)
	public List<EplStatement> getEplStatements() {
		return epService.getStatements();
	}

	@ApiOperation(value = "Get esper schema (event type) identified by name")
	@RequestMapping(value = "schema/{name}", method = RequestMethod.GET, headers = "Accept=application/json")
	@ResponseStatus(value = HttpStatus.OK)
	public EplSchema getEplSchema(@PathVariable("name") String name) {
		if (epService.hasEPLSchema(name)) {
			return epService.getEPLSchema(name);
		}
		return null;
	}

	@ApiOperation(value = "Add new esper schema (event type)")
	@RequestMapping(value = "schema", method = RequestMethod.POST, headers = "Accept=application/json")
	@ResponseStatus(value = HttpStatus.CREATED)
	public String addEplSchema(@RequestBody EplSchema schema) {
		epService.addEPLSchema(schema);
		return schema.name;
	}

	@ApiOperation(value = "Delete esper schema (event type) identified by name")
	@RequestMapping(value = "schema/{name}", method = RequestMethod.DELETE, headers = "Accept=application/json")
	@ResponseStatus(value = HttpStatus.NO_CONTENT)
	public void deleteEplSchema(@PathVariable("name") String name) {
		if (!epService.hasEPLSchema(name)) {
			throw new IllegalArgumentException();
		}
		epService.removeEPLSchema(name);
	}

	@ApiOperation(value = "Get all defined esper schemas (event types)")
	@RequestMapping(value = "schemas", method = RequestMethod.GET, headers = "Accept=application/json")
	@ResponseStatus(value = HttpStatus.OK)
	public List<EplSchema> getEplSchemas() {
		return epService.getEPLSchemas();
	}

	@ApiOperation(value = "Send a single event given as map/json object")
	@RequestMapping(value = "sendEvent/map", method = RequestMethod.POST, headers = "Accept=application/json")
	@ResponseStatus(value = HttpStatus.OK)
	public void sendEvent(@RequestBody EplEvent event) {
		epService.sendEPLEvent(event);
	}

}
