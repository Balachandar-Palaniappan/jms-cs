package com.payoneer.cs.error;

import org.quartz.CronExpression;
import org.springframework.stereotype.Component;

import com.payoneer.cs.job.model.Job;
import com.payoneer.cs.job.model.JobExecutionType;
import com.payoneer.cs.job.util.JobResponseErrorCode;

@Component
public class ModelValidator {

	public boolean validateJobModel(Job job) throws AppResponseException {
		if (job.getJobName().isBlank() || null == job.getJobName()) {
			throw new AppResponseException(JobResponseErrorCode.RESPONSE_ERROR_004);
		}
		if (job.getJobGroupName().isBlank() || null == job.getJobGroupName()) {
			throw new AppResponseException(JobResponseErrorCode.RESPONSE_ERROR_005);
		}

		if (job.getSchedule().getExecutionType().toString().isBlank()
				|| null == job.getSchedule().getExecutionType().toString()) {
			throw new AppResponseException(JobResponseErrorCode.RESPONSE_ERROR_006);
		}
		if (job.getSchedule().getExecutionType().equals(JobExecutionType.SCHEDULED)
				&& null == job.getSchedule().getScheduleDateTime()) {
			throw new AppResponseException(JobResponseErrorCode.RESPONSE_ERROR_005);
		}

		if (job.getSchedule().getExecutionType().equals(JobExecutionType.SCHEDULED) && job.getCronExpression() != null
				&& !CronExpression.isValidExpression(job.getCronExpression())) {
			throw new AppResponseException(JobResponseErrorCode.RESPONSE_ERROR_008);
		}

		return true;
	}

}
