package com.payoneer.cs.job.model;

import java.util.Date;

import org.springframework.format.annotation.DateTimeFormat;

import com.fasterxml.jackson.annotation.JsonInclude;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobSchedule {
	@Schema(description = "Type of execution")
	private JobExecutionType executionType;
	@Schema(description = "Scheduled date and time (yyyy-MM-dd HH:mm:ss)")
	@DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
	private Date scheduleDateTime;
}
