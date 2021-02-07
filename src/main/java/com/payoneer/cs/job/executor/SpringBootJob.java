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

import lombok.extern.log4j.Log4j2;

@Component
@PersistJobDataAfterExecution
@Log4j2
public class SpringBootJob extends QuartzJobBean implements InterruptableJob {

	@Autowired
	private JobUtil jobUtil;
	@Autowired
	private JobRepository jobRepository;

	@Override
	protected void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		JobKey key = jobExecutionContext.getJobDetail().getKey();
		log.debug("Simple Job started with key :" + key.getName() + ", Group :" + key.getGroup() + " , Thread Name :"
				+ Thread.currentThread().getName());
		log.debug("======================================");

		JobDataMap dataMap = jobExecutionContext.getMergedJobDataMap();
		String jobJson = (String) dataMap.get(key.getName().concat(key.getGroup()));
		Optional<JobData> oJobData = jobRepository.findById(key.getName());
		if (oJobData.isPresent()) {
			JobData jobData = oJobData.get();
			try {
				String executionString = this.getJobExecutionExpression(key.getName(), jobJson);

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
		}
		log.debug("Thread: " + Thread.currentThread().getName() + " stopped.");
		log.debug("======================================");
	}

	private String getJobExecutionExpression(String jobId, String jobJson) throws JobProcessingException {
		Optional<Job> oJob = jobUtil.getJobData(jobJson);
		if (oJob.isPresent()) {
			Job job = oJob.get();
			return Stream
					.of("java", job.getEnvironmentString(), "-jar", job.getFileLocation(), job.getId(),
							job.getParameters())
					.filter(token -> token != null && !token.isEmpty()).collect(Collectors.joining(" "));
		} else {
			throw new JobProcessingException(jobId, JobMessageCode.MESSAGE_002);
		}
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		log.debug("Stopping thread... ");
	}

	private void updateStatus(JobData jobData, String jobStatus) {
		jobData.setStatus(jobStatus);
		jobRepository.save(jobData);
	}

}