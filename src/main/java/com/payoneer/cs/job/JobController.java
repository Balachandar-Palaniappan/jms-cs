package com.payoneer.cs.job;

import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.payoneer.cs.job.model.Job;
import com.payoneer.cs.job.model.JobExecutionType;
import com.payoneer.cs.job.model.JobType;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("/api")
@Tag(name = "Job Management", description = "Submit/Retrieve Job Operations")
public class JobController {
	@Autowired
	@Lazy
	private JobService jobService;

	@PostMapping(path = "/v1/job", consumes = "multipart/form-data")
	@Operation(summary = "Submit a job to be executed")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "201", description = "Contact created", content = @Content(schema = @Schema(implementation = URI.class))),
			@ApiResponse(responseCode = "400", description = "Invalid input") })
	public ResponseEntity<?> submitJob(@RequestParam("jobName") String jobName,
			@RequestParam("jobGroupName") String jobGroupName, @RequestParam("file") MultipartFile file,
			@RequestParam("jobType") JobType jobType,
			@RequestParam("executionType") JobExecutionType executionType,
			@RequestParam(name = "schedule", required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date date,
			@RequestParam(name = "cronExpression", required = false) String cronExpression,
			@RequestParam(name = "environmentString", required = false) String environmentString,
			@RequestParam(name = "parameters", required = false) String parameters,
			@RequestParam(name = "priority", required = false, defaultValue = "5") Integer priority,
			@RequestParam(name = "repeatTime", required = false) Long repeatTime) {

		URI uri = ServletUriComponentsBuilder.fromCurrentRequestUri().path("/{id}")
				.buildAndExpand(jobService.submitJob(jobName, jobGroupName, file, jobType, executionType, date,
						priority, parameters, environmentString, cronExpression, repeatTime))
				.toUri();
		return ResponseEntity.created(uri).build();
	}

	@GetMapping("/v1/job")
	@Operation(summary = "Retrieve all jobs")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "successful operation", content = @Content(schema = @Schema(implementation = Job.class))),
			@ApiResponse(responseCode = "204", description = "Contact not found") })
	public ResponseEntity<?> retrieveAllJobs() {
		List<Job> jobs = jobService.retrieveAllJobs();
		return jobs.isEmpty() ? ResponseEntity.noContent().build() : ResponseEntity.ok(jobs);
	}

	@GetMapping("/v1/job/{id}")
	@Operation(summary = "Retrieve a job to by id")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "successful operation", content = @Content(schema = @Schema(implementation = Job.class))),
			@ApiResponse(responseCode = "404", description = "Contact not found") })
	public ResponseEntity<?> retrieveJob(@PathVariable("id") String id) {
		Optional<Job> job = jobService.retrieveJob(id);
		return job.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(job.get());
	}

}
