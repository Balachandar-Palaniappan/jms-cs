package com.payoneer.cs.job;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.payoneer.cs.error.ModelValidator;
import com.payoneer.cs.job.model.Job;
import com.payoneer.cs.job.model.JobExecutionType;
import com.payoneer.cs.job.model.JobSchedule;
import com.payoneer.cs.job.model.JobStatus;
import com.payoneer.cs.job.model.JobType;
import com.payoneer.cs.job.service.SchedulerServiceImpl;
import com.payoneer.cs.store.StorageService;

@Service
public class JobService {

	@Autowired
	private SchedulerServiceImpl schedulerService;

	@Autowired
	private StorageService storageService;

	@Autowired
	private ModelValidator modelValidator;

	public String submitJob(String jobName, String jobGroupName, MultipartFile file, JobType jobType,
			JobExecutionType executionType, Date date, Integer priority, String parameters, String environmentString,
			String cronExpression, Long repeatTime) {

		String jobId = UUID.randomUUID().toString();

		String fileLocation = storageService.saveFile(file, jobId);

		Job job = Job.builder().id(jobId).fileLocation(fileLocation).status(JobStatus.QUEUED).type(jobType)
				.schedule(new JobSchedule(executionType, date)).priority(priority).parameters(parameters)
				.jobGroupName(jobGroupName).jobName(jobName).environmentString(environmentString).build();

		modelValidator.validateJobModel(job);

		return schedulerService.submitJob(job);
	}

	public List<Job> retrieveAllJobs() {
		return schedulerService.getAllJobs();
	}

	public Optional<Job> retrieveJob(String id) {
		return schedulerService.getJobById(id);
	}

}
