package com.payoneer.cs.job.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.quartz.CronExpression;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Service;

import com.payoneer.cs.config.JobScheduleCreator;
import com.payoneer.cs.error.AppResponseException;
import com.payoneer.cs.job.executor.SpringBootJob;
import com.payoneer.cs.job.model.Job;
import com.payoneer.cs.job.model.JobData;
import com.payoneer.cs.job.model.JobExecutionType;
import com.payoneer.cs.job.model.JobSchedule;
import com.payoneer.cs.job.model.JobStatus;
import com.payoneer.cs.job.model.JobType;
import com.payoneer.cs.job.util.JobResponseErrorCode;
import com.payoneer.cs.repository.JobRepository;

import lombok.extern.log4j.Log4j2;

@Service
@Log4j2
public class SchedulerServiceImpl implements IScheduleService {

	@Autowired
	private SchedulerFactoryBean schedulerFactoryBean;

	@Autowired
	private ApplicationContext context;

	@Autowired
	private JobScheduleCreator scheduleCreator;

	@Autowired
	private JobRepository jobRepository;

	@Override
	public String submitJob(Job job) throws AppResponseException {

		JobDetail jobDetail = scheduleCreator.createJob(SpringBootJob.class, true, context, job);

		return this.scheduleJob(jobDetail, job);
	}

	private String scheduleJob(JobDetail jobDetail, Job job) {
		Scheduler scheduler = schedulerFactoryBean.getScheduler();
		try {
			JobData jobData = buildJobDataModel(job);
			String jobId = jobRepository.save(jobData).getId();
			Trigger trigger = this.getTrigger(job);
			scheduler.scheduleJob(jobDetail, trigger);
			return jobId;
		} catch (SchedulerException schedulerException) {
			jobRepository.deleteById(job.getId());
			throw new AppResponseException(JobResponseErrorCode.RESPONSE_ERROR_003);
		}
	}

	private Trigger getTrigger(Job job) {
		Trigger trigger = TriggerBuilder.newTrigger().build();
		if (JobExecutionType.IMMEDIATE.equals(job.getSchedule().getExecutionType())) {
			trigger = TriggerBuilder.newTrigger().withIdentity(job.getId(), job.getJobGroupName()).build();
		} else {
			if (Objects.isNull(job.getCronExpression()) || job.getCronExpression().isBlank()) {
				trigger = scheduleCreator.createSimpleTrigger(job.getId(), job.getJobGroupName(),
						job.getSchedule().getScheduleDateTime(), job.getRepeatTime(),
						SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW, job.getPriority());
			} else if (CronExpression.isValidExpression(job.getCronExpression())) {
				trigger = scheduleCreator.createCronTrigger(job.getId(), job.getJobGroupName(),
						job.getSchedule().getScheduleDateTime(), job.getCronExpression(),
						SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW, job.getPriority());
			}
		}
		return trigger;
	}

	/**
	 * Check if job is already running
	 */
	public boolean isJobRunning(String jobId, String jobGroupName, Scheduler scheduler) {
		try {
			List<JobExecutionContext> currentJobs = scheduler.getCurrentlyExecutingJobs();
			if (currentJobs != null && !currentJobs.isEmpty()) {
				for (JobExecutionContext jobCtx : currentJobs) {
					String jobNameDB = jobCtx.getJobDetail().getKey().getName();
					String groupNameDB = jobCtx.getJobDetail().getKey().getGroup();
					if (jobId.equalsIgnoreCase(jobNameDB) && jobGroupName.equalsIgnoreCase(groupNameDB)) {
						return true;
					}
				}
			}
		} catch (SchedulerException schedulerException) {
			log.error("SchedulerException while checking job with key :" + jobId + " is running. error message : {}",
					schedulerException.getMessage());
			return false;
		}
		return false;
	}

	/**
	 * Get the current state of job
	 */
	public JobStatus getJobState(String jobId, String jobGroupName, String jobStatus) {
		try {
			Scheduler scheduler = schedulerFactoryBean.getScheduler();
			boolean isRunning = isJobRunning(jobId, jobGroupName, scheduler);
			if (isRunning) {
				return JobStatus.RUNNING;
			} else {
				JobKey jobKey = new JobKey(jobId, jobGroupName);
				JobDetail jobDetail = scheduler.getJobDetail(jobKey);
				return Objects.isNull(jobDetail) ? JobStatus.valueOf(jobStatus)
						: getTriggerStatus(jobStatus, scheduler, jobDetail);
			}
		} catch (SchedulerException schedulerException) {
			log.error("SchedulerException while checking job with id and group exist:{}",
					schedulerException.getMessage());
		}
		return JobStatus.FAILED;
	}

	private JobStatus getTriggerStatus(String jobStatus, Scheduler scheduler, JobDetail jobDetail)
			throws SchedulerException {
		List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobDetail.getKey());
		if (triggers != null && !triggers.isEmpty()) {
			for (Trigger trigger : triggers) {
				TriggerState triggerState = scheduler.getTriggerState(trigger.getKey());
				if (TriggerState.COMPLETE.equals(triggerState)) {
					return JobStatus.SUCCESS;
				} else if (TriggerState.NONE.equals(triggerState) || TriggerState.ERROR.equals(triggerState)) {
					return JobStatus.FAILED;
				} else if (TriggerState.PAUSED.equals(triggerState) || TriggerState.NORMAL.equals(triggerState)
						|| TriggerState.BLOCKED.equals(triggerState)) {
					return JobStatus.QUEUED;
				}
			}
		}
		return JobStatus.valueOf(jobStatus);
	}

	@Override
	public Optional<Job> getJobById(String id) {
		Optional<JobData> oJobData = jobRepository.findById(id);
		if (!oJobData.isEmpty()) {
			JobData jobData = oJobData.get();
			return Optional.of(buildJobModel(jobData));
		} else {
			return Optional.empty();
		}

	}

	private Job buildJobModel(JobData jobData) {
		JobSchedule schedule = new JobSchedule();
		schedule.setExecutionType(JobExecutionType.valueOf(jobData.getJobExecutionType()));
		schedule.setScheduleDateTime(jobData.getSchedule());
		return Job.builder().id(jobData.getId()).jobName(jobData.getJobName()).jobGroupName(jobData.getJobGroupName())
				.cronExpression(jobData.getCronExpression()).environmentString(jobData.getEnvironmentString())
				.fileLocation(jobData.getFileLocation()).parameters(jobData.getParameters())
				.priority(jobData.getPriority()).repeatTime(jobData.getRepeatTime()).schedule(schedule)
				.type(JobType.valueOf(jobData.getJobType())).status(JobStatus.valueOf(jobData.getStatus())).build();
	}

	private JobData buildJobDataModel(Job job) {
		return JobData.builder().id(job.getId()).jobName(job.getJobName())
				.jobGroupName(job.getJobGroupName()).fileLocation(job.getFileLocation())
				.status(JobStatus.QUEUED.toString()).jobType(job.getType().toString()).priority(job.getPriority())
				.parameters(job.getParameters()).environmentString(job.getEnvironmentString())
				.cronExpression(job.getCronExpression()).repeatTime(job.getRepeatTime())
				.schedule(job.getSchedule().getScheduleDateTime())
				.jobExecutionType(job.getSchedule().getExecutionType().toString()).build();
	}

	@Override
	public List<Job> getAllJobs() {
		List<JobData> jobDatas = jobRepository.findAll();
		List<Job> jobs = new ArrayList<>();

		jobDatas.stream().forEach(jobData -> {
			JobStatus jobStatus = getJobState(jobData.getId(), jobData.getJobGroupName(), jobData.getStatus());
			jobData.setStatus(jobStatus.toString());
			jobs.add(buildJobModel(jobData));
		});

		return jobs;
	}

	@Override
	public boolean updateScheduleJob(Job job) {
		try {
			Trigger trigger = null;
			if (Objects.isNull(job.getCronExpression()) || job.getCronExpression().isBlank()) {
				trigger = scheduleCreator.createSimpleTrigger(job.getId(), job.getJobGroupName(),
						job.getSchedule().getScheduleDateTime(), job.getRepeatTime(),
						SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW, job.getPriority());
			} else if (CronExpression.isValidExpression(job.getCronExpression())) {
				trigger = scheduleCreator.createCronTrigger(job.getId(), job.getJobGroupName(),
						job.getSchedule().getScheduleDateTime(), job.getCronExpression(),
						SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW, job.getPriority());
			}
			Date reScheduledDate = schedulerFactoryBean.getScheduler()
					.rescheduleJob(TriggerKey.triggerKey(job.getId(), job.getJobGroupName()), trigger);
			if (reScheduledDate != null) {
				jobRepository.save(buildJobDataModel(job));
				return true;
			}
		} catch (SchedulerException e) {
			log.error("Failed to reschedule a Job: {}", e);
		}
		return false;
	}

	@Override
	public boolean deleteJob(Job job) {
		try {
			jobRepository.deleteById(job.getId());
			return schedulerFactoryBean.getScheduler().deleteJob(new JobKey(job.getId(), job.getJobGroupName()));
		} catch (SchedulerException e) {
			log.error("Failed to delete job - {}", job.getJobName(), e);
			return false;
		}
	}

}