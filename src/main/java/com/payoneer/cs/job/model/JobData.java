package com.payoneer.cs.job.model;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(catalog = "jms_service_db", name = "job_info")
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(Include.NON_NULL) 
public class JobData {
	@Id
	private String id;
	private String jobType;
	private String status;
	private String fileLocation;
	private Integer priority;
	private String jobExecutionType;
	private Date schedule;
	private String parameters;
	private String environmentString;
	private String jobGroupName;
	private String jobName;
	private String cronExpression;
	private Long repeatTime;
	private Date createdDate;
	private Date lastModifiedDate;
}
