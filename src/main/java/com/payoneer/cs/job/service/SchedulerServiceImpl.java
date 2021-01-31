package com.payoneer.cs.job.service;

import java.util.ArrayList;
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

		JobData jobData = buildJobDataModel(job);

		String jobId = jobRepository.save(jobData).getId();

		this.scheduleJob(jobDetail, job);

		return jobId;
	}

	private void scheduleJob(JobDetail jobDetail, Job job) {
		Scheduler scheduler = schedulerFactoryBean.getScheduler();
		try {
			Trigger trigger = this.getTrigger(jobDetail, job);
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (SchedulerException schedulerException) {
			throw new AppResponseException(JobResponseErrorCode.RESPONSE_ERROR_003);
		}
	}

	private Trigger getTrigger(JobDetail jobDetail, Job job) {
		Trigger trigger = TriggerBuilder.newTrigger().build();
		if (JobExecutionType.IMMEDIATE.equals(job.getSchedule().getExecutionType())) {
			trigger = TriggerBuilder.newTrigger().withIdentity(job.getJobName(), job.getJobGroupName()).build();
		} else {
			if (Objects.isNull(job.getCronExpression()) || job.getCronExpression().isBlank()) {
				trigger = scheduleCreator.createSimpleTrigger(job.getId(), job.getSchedule().getScheduleDateTime(),
						job.getRepeatTime(), SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW, job.getPriority());
			} else if (CronExpression.isValidExpression(job.getCronExpression())) {
				trigger = scheduleCreator.createCronTrigger(job.getId(), job.getSchedule().getScheduleDateTime(),
						job.getCronExpression(), SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW, job.getPriority());
			}
		}
		return trigger;
	}


	public boolean isJobRunning(String jobId, String jobGroupName, Scheduler scheduler) {
		try {
			List<JobExecutionContext> currentJobs = scheduler.getCurrentlyExecutingJobs();
			if (currentJobs != null) {
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

	public JobStatus getJobState(String jobId, String jobGroupName) {
		try {
			Scheduler scheduler = schedulerFactoryBean.getScheduler();
			boolean isRunning = isJobRunning(jobId, jobGroupName, scheduler);
			if (isRunning) {
				return JobStatus.RUNNING;
			} else {
				JobKey jobKey = new JobKey(jobId, jobGroupName);
				JobDetail jobDetail = scheduler.getJobDetail(jobKey);
				List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobDetail.getKey());
				if (triggers != null && triggers.size() > 0) {
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
			}
		} catch (SchedulerException schedulerException) {
			log.error("SchedulerException while checking job with id and group exist:{}",
					schedulerException.getMessage());
		}
		return JobStatus.FAILED;
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
		JobData jobData = JobData.builder().id(job.getId()).jobName(job.getJobName())
				.jobGroupName(job.getJobGroupName()).fileLocation(job.getFileLocation())
				.status(JobStatus.QUEUED.toString()).jobType(job.getType().toString()).priority(job.getPriority())
				.parameters(job.getParameters()).environmentString(job.getEnvironmentString())
				.cronExpression(job.getCronExpression()).repeatTime(job.getRepeatTime())
				.schedule(job.getSchedule().getScheduleDateTime())
				.jobExecutionType(job.getSchedule().getExecutionType().toString()).build();
		return jobData;
	}

	@Override
	public List<Job> getAllJobs() {
		List<JobData> jobDatas = jobRepository.findAll();
		List<Job> jobs = new ArrayList<>();

		jobDatas.stream().forEach(jobData -> {
			jobs.add(buildJobModel(jobData));
		});

		return jobs;
	}

}
