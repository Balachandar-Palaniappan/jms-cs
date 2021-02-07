package com.payoneer.cs.job.error;

import org.quartz.JobExecutionException;

import com.payoneer.cs.job.util.JobMessageCode;

import lombok.Getter;

@Getter
public class JobProcessingException extends JobExecutionException {
	private static final long serialVersionUID = 817091929544865992L;

	private final String jobId;
	private final JobMessageCode jobMessageCode;

	public JobProcessingException(String jobId, JobMessageCode jobMessageCode, Exception exception) {
		super(exception);
		this.jobId = jobId;
		this.jobMessageCode = jobMessageCode;
	}

	public JobProcessingException(String jobId, JobMessageCode jobMessageCode) {
		this.jobId = jobId;
		this.jobMessageCode = jobMessageCode;
	}
}
