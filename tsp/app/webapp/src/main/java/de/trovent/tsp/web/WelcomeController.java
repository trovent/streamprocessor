package de.trovent.tsp.web;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import de.trovent.tsp.web.dto.AppStatus;

@RestController
public class WelcomeController {
	
	private Logger logger = LogManager.getLogger();
	
	@GetMapping("/status")
	AppStatus status() {
		logger.info("Getting status of the Rest App");
		return new AppStatus("App", "OK");
	}

}
