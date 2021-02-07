package com.payoneer.cs.error;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AppResponseException extends RuntimeException {
	private static final long serialVersionUID = 4685054917599830536L;
	
	private final AppResponseErrorCode appResponseErrorCode;
}
