package com.payoneer.cs.job.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpStatus;

import com.payoneer.cs.error.AppResponseErrorCode;

@Getter
@AllArgsConstructor
public enum JobResponseErrorCode implements AppResponseErrorCode {
	RESPONSE_ERROR_001("Failed to submit job : Empty job file", HttpStatus.BAD_REQUEST),
	RESPONSE_ERROR_002("Failed to submit job : Error saving file", HttpStatus.BAD_REQUEST),
	RESPONSE_ERROR_003("Failed to submit job : Error scheduling job", HttpStatus.BAD_REQUEST),
	RESPONSE_ERROR_004("Failed to submit job : Provide Job name", HttpStatus.BAD_REQUEST),
	RESPONSE_ERROR_005("Failed to submit job : Provide Job group name", HttpStatus.BAD_REQUEST),
	RESPONSE_ERROR_006("Failed to submit job : Scheduled Job should be provided with date", HttpStatus.BAD_REQUEST),
	RESPONSE_ERROR_007("Failed to submit job : Execution type should be provide", HttpStatus.BAD_REQUEST),
	RESPONSE_ERROR_008("Failed to submit job : Invalid cron expression", HttpStatus.BAD_REQUEST);

	private String message;
	private HttpStatus httpStatus;
}