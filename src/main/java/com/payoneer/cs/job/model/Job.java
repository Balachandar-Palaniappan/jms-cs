package com.payoneer.cs.job.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Job {
	@Schema(description = "The auto-generated job id")
	private String id;
	@Schema(description = "Type of job")
	private JobType type;
	@Schema(description = "Current status of job")
	private JobStatus status;
	@Schema(description = "Location of job file")
	private String fileLocation;
	@Schema(description = "Priority of job")
	private Integer priority;
	@Schema(description = "JobSchedule of job")
	private JobSchedule schedule;
	@Schema(description = "Parameters for the job")
	private String parameters;
	@Schema(description = "Environment for the job")
	private String environmentString;
	@Schema(description = "Job group name for the job")
	private String jobGroupName;
	@Schema(description = "Job name for the job")
	private String jobName;
	@Schema(description = "Cron expression for the job")
	private String cronExpression;
	@Schema(description = "RepeatTime for the job")
	private Long repeatTime;
}
