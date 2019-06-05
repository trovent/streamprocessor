package de.trovent.tsp.web.exceptionHandling;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import com.espertech.esper.client.EPException;

import de.trovent.tsp.web.exceptionHandling.dto.CustomError;

@ControllerAdvice
public class RestResponseEntityExceptionHandler extends ResponseEntityExceptionHandler {
	
	@ExceptionHandler(value = { IllegalArgumentException.class })
	protected ResponseEntity<Object> handleIllegalArgument(RuntimeException ex, WebRequest request) {
		    	
    	CustomError error = new CustomError(
    			HttpStatus.BAD_REQUEST.getReasonPhrase(), 
    			ex.getMessage());
    	
    	return handleExceptionInternal(ex, error, new HttpHeaders(), HttpStatus.BAD_REQUEST, request);
	}
	
	@ExceptionHandler(value = { EPException.class })
	protected ResponseEntity<Object> handleEPException(RuntimeException ex, WebRequest request) {
		    	
    	CustomError error = new CustomError(
    			HttpStatus.BAD_REQUEST.getReasonPhrase(), 
    			ex.getMessage());
    	
    	return handleExceptionInternal(ex, error, new HttpHeaders(), HttpStatus.BAD_REQUEST, request);
	}
	
}
