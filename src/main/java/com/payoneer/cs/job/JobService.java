package com.payoneer.cs.job;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.payoneer.cs.error.ModelValidator;
import com.payoneer.cs.job.model.Job;
import com.payoneer.cs.job.service.SchedulerServiceImpl;
import com.payoneer.cs.store.StorageService;

import javassist.NotFoundException;

@Service
public class JobService {

	@Autowired
	private SchedulerServiceImpl schedulerService;

	@Autowired
	private StorageService storageService;

	@Autowired
	private ModelValidator modelValidator;

	public String submitJob(Job job, MultipartFile file) {

		String jobId = UUID.randomUUID().toString();

		String fileLocation = storageService.saveFile(file, jobId);
		job.setId(jobId);
		job.setFileLocation(fileLocation);

		modelValidator.validateJobModel(job);

		return schedulerService.submitJob(job);
	}

	public boolean updateJob(String jobId, Date date, Integer priority, String parameters, String environmentString,
			String cronExpression, Long repeatTime) throws NotFoundException {
		Optional<Job> oJob = this.retrieveJob(jobId);
		if (oJob.isPresent()) {
			Job job = oJob.get();
			modelValidator.validateJobModel(job);
			getUpdatedJobDetails(date, priority, parameters, environmentString, cronExpression, repeatTime, job);
			return schedulerService.updateScheduleJob(job);
		} else {
			throw new NotFoundException("Job not Found");
		}

	}

	private void getUpdatedJobDetails(Date date, Integer priority, String parameters, String environmentString,
			String cronExpression, Long repeatTime, Job job) {
		job.setCronExpression((Objects.nonNull(cronExpression) && !cronExpression.isEmpty()) ? cronExpression
				: job.getCronExpression());
		job.setRepeatTime((Objects.nonNull(repeatTime) && repeatTime != 0l) ? repeatTime : job.getRepeatTime());
		job.setParameters(Objects.nonNull(parameters) ? parameters : job.getParameters());
		job.setEnvironmentString(Objects.nonNull(environmentString) ? environmentString : job.getEnvironmentString());
		job.getSchedule().setScheduleDateTime(Objects.nonNull(date) ? date : job.getSchedule().getScheduleDateTime());
		job.setPriority(Objects.nonNull(priority) ? priority : job.getPriority());
	}

	public List<Job> retrieveAllJobs() {
		return schedulerService.getAllJobs();
	}

	public Optional<Job> retrieveJob(String id) {
		return schedulerService.getJobById(id);
	}

	public boolean deletJob(String jobId) throws NotFoundException {
		Optional<Job> oJob = this.retrieveJob(jobId);
		if (oJob.isPresent()) {
			Job job = oJob.get();
			return schedulerService.deleteJob(job);
		} else {
			throw new NotFoundException("Job not Found");
		}
	}

}
