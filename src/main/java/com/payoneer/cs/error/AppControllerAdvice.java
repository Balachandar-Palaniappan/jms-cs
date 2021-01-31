package com.payoneer.cs.error;

import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import com.payoneer.cs.job.JobController;

@Log4j2
@ControllerAdvice(assignableTypes = { JobController.class })
public class AppControllerAdvice {
	@ExceptionHandler({ RuntimeException.class })
	public ResponseEntity<AppResponseError> handleRunTimeException(RuntimeException exception) {
		log.error(exception.getMessage());

		return new ResponseEntity<>(new AppResponseError(exception.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
	}

	@ExceptionHandler({ AppResponseException.class })
	public ResponseEntity<AppResponseError> handleAppException(AppResponseException exception) {
		log.error(exception.getMessage());
		return new ResponseEntity<>(new AppResponseError(exception.getAppResponseErrorCode().getMessage()),
				exception.getAppResponseErrorCode().getHttpStatus());
	}
}
