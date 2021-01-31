package com.payoneer.cs.job.executor;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.UnableToInterruptJobException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.payoneer.cs.job.error.JobProcessingException;
import com.payoneer.cs.job.model.Job;
import com.payoneer.cs.job.model.JobData;
import com.payoneer.cs.job.model.JobStatus;
import com.payoneer.cs.job.util.JobMessageCode;
import com.payoneer.cs.job.util.JobUtil;
import com.payoneer.cs.repository.JobRepository;

@Component
@PersistJobDataAfterExecution
public class SpringBootJob extends QuartzJobBean implements InterruptableJob {

	@Autowired
	private JobUtil jobUtil;
	@Autowired
	private JobRepository jobRepository;

	@Override
	protected void executeInternal(JobExecutionContext jobExecutionContext)
			throws JobExecutionException, JobProcessingException {
		JobKey key = jobExecutionContext.getJobDetail().getKey();
		System.out.println("Simple Job started with key :" + key.getName() + ", Group :" + key.getGroup()
				+ " , Thread Name :" + Thread.currentThread().getName());
		System.out.println("======================================");

		JobDataMap dataMap = jobExecutionContext.getMergedJobDataMap();
		String jobJson = (String) dataMap.get(key.getName().concat(key.getGroup()));

		Optional<JobData> oJobData = jobRepository.findById(key.getName());
		JobData jobData = oJobData.get();

		try {

			String executionString = this.getJobExecutionExpression(jobJson);

			this.updateStatus(jobData, JobStatus.RUNNING.toString());

			if (Runtime.getRuntime().exec(executionString).waitFor() != 0)
				throw new JobProcessingException(jobData.getId(), JobMessageCode.MESSAGE_002);

		} catch (InterruptedException interruptedException) {
			Thread.currentThread().interrupt();
			this.updateStatus(jobData, JobStatus.FAILED.toString());
		} catch (IOException exception) {
			this.updateStatus(jobData, JobStatus.FAILED.toString());
			throw new JobProcessingException(jobData.getId(), JobMessageCode.MESSAGE_002, exception);
		}

		this.updateStatus(jobData, JobStatus.SUCCESS.toString());
		System.out.println("Thread: " + Thread.currentThread().getName() + " stopped.");
		System.out.println("======================================");
	}

	private String getJobExecutionExpression(String jobJson) {
		Optional<Job> oJob = jobUtil.getJobData(jobJson);
		Job job = oJob.get();
		String executionString = Stream
				.of("java", job.getEnvironmentString(), "-jar", job.getFileLocation(), job.getId(), job.getParameters())
				.filter(token -> token != null && !token.isEmpty()).collect(Collectors.joining(" "));
		return executionString;
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		System.out.println("Stopping thread... ");
	}

	private void updateStatus(JobData jobData, String jobStatus) {
		jobData.setStatus(jobStatus);
		jobRepository.save(jobData);
	}

}