package com.payoneer.cs.job.util;

import java.util.Optional;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payoneer.cs.job.model.Job;

import lombok.extern.log4j.Log4j2;

@Component
@Log4j2
public class JobUtil {

	public Optional<Job> getJobData(String jobJson) {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return Optional.of(objectMapper.readValue(jobJson, Job.class));
		} catch (JsonMappingException jsonMappingException) {
			log.error("Exception Occured: {}", jsonMappingException);
		} catch (JsonProcessingException jsonProcessingException) {
			log.error("Exception Occured: {}", jsonProcessingException);
		}
		return Optional.empty();
	}

	public String getJson(Job job) {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.writeValueAsString(job).toString();
		} catch (JsonMappingException jsonMappingException) {
			log.error("Exception Occured: {}", jsonMappingException);
		} catch (JsonProcessingException jsonProcessingException) {
			log.error("Exception Occured: {}", jsonProcessingException);
		}
		return "";
	}

}
