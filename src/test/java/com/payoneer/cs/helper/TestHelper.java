package com.payoneer.cs.helper;

import java.io.IOException;
import java.nio.file.Files;

import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;

import com.payoneer.cs.job.model.JobExecutionType;
import com.payoneer.cs.job.model.JobType;

@Component
public class TestHelper {
	private byte[] getFile() {
		try {
			return Files.readAllBytes(new ClassPathResource("jm-job-logger-1.0.jar").getFile().toPath());
		} catch (IOException e) {
			return null;
		}
	}

	public MultipartBodyBuilder submitJobRequestData() {
		MultipartBodyBuilder multipartBodyBuilder = new MultipartBodyBuilder();

		multipartBodyBuilder.part("file", getFile())
				.header("Content-Disposition", "form-data; name=file; filename=jm-job-logger-1.0.jar")
				.header("Content-Type", "multipart/form-data");
		multipartBodyBuilder.part("jobType", JobType.SPRING_BOOT_JAR.toString());
		multipartBodyBuilder.part("executionType", JobExecutionType.IMMEDIATE.toString());
		multipartBodyBuilder.part("jobGroupName", "TestGroupName");
		multipartBodyBuilder.part("jobName", "TestPostJob");
		multipartBodyBuilder.part("parameters", "TestPostJob");
		return multipartBodyBuilder;
	}

	public MultipartBodyBuilder submitJobRequestData_MissingJobFile() {
		MultipartBodyBuilder multipartBodyBuilder = new MultipartBodyBuilder();

		multipartBodyBuilder.part("jobType", JobType.SPRING_BOOT_JAR.toString());
		multipartBodyBuilder.part("executionType", JobExecutionType.IMMEDIATE.toString());

		return multipartBodyBuilder;
	}

	public HttpEntity<LinkedMultiValueMap<String, Object>> getEntity() {
		LinkedMultiValueMap<String, Object> parameters = new LinkedMultiValueMap<String, Object>();
		parameters.add("file", submitJobRequestData().build());
		
		parameters.add("jobType", JobType.SPRING_BOOT_JAR.toString());
		parameters.add("executionType", JobExecutionType.IMMEDIATE.toString());
		parameters.add("jobGroupName", "TestGroupName");
		parameters.add("jobName", "TestPostJob");

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.MULTIPART_FORM_DATA);

		return new HttpEntity<LinkedMultiValueMap<String, Object>>(parameters, headers);

	}

}
