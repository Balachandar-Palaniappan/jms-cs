package com.payoneer.cs.config;

import java.text.ParseException;
import java.util.Date;

import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.SimpleTrigger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.scheduling.quartz.SimpleTriggerFactoryBean;
import org.springframework.stereotype.Component;

import com.payoneer.cs.job.model.Job;
import com.payoneer.cs.job.util.JobUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class JobScheduleCreator {

	@Autowired
	private JobUtil jobUtil;

	/**
	 * Create Quartz Job.
	 *
	 * @param jobClass  Class whose executeInternal() method needs to be called.
	 * @param isDurable Job needs to be persisted even after completion. if true,
	 *                  job will be persisted, not otherwise.
	 * @param context   Spring application context.
	 * @param jobName   Job name.
	 * @param jobGroup  Job group.
	 * @return JobDetail object
	 */
	public JobDetail createJob(Class<? extends QuartzJobBean> jobClass, boolean isDurable, ApplicationContext context,
			Job job) {
		JobDetailFactoryBean factoryBean = new JobDetailFactoryBean();
		factoryBean.setJobClass(jobClass);
		factoryBean.setDurability(isDurable);
		factoryBean.setApplicationContext(context);
		factoryBean.setName(job.getId());
		factoryBean.setGroup(job.getJobGroupName());

		// set job data map
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(job.getId().concat(job.getJobGroupName()), jobUtil.getJson(job));
		factoryBean.setJobDataMap(jobDataMap);

		factoryBean.afterPropertiesSet();

		return factoryBean.getObject();
	}

	public JobDetail getJobDetail(Job job, Class<? extends QuartzJobBean> jobClass) {

		JobDetail jobDetail = JobBuilder.newJob(jobClass).withIdentity(job.getId(), job.getJobGroupName()).build();
		jobDetail.getJobDataMap().put(job.getId().concat(job.getJobGroupName()), jobUtil.getJson(job));
		return jobDetail;
	}

	/**
	 * Create cron trigger.
	 *
	 * @param triggerName        Trigger name.
	 * @param startTime          Trigger start time.
	 * @param cronExpression     Cron expression.
	 * @param misFireInstruction Misfire instruction (what to do in case of misfire
	 *                           happens).
	 * @return {@link CronTrigger}
	 */
	public CronTrigger createCronTrigger(String triggerName,String groupName, Date startTime, String cronExpression,
			int misFireInstruction, int priority) {
		CronTriggerFactoryBean factoryBean = new CronTriggerFactoryBean();
		factoryBean.setName(triggerName);
		factoryBean.setGroup(groupName);
		factoryBean.setStartTime(startTime);
		factoryBean.setCronExpression(cronExpression);
		factoryBean.setMisfireInstruction(misFireInstruction);
		factoryBean.setPriority(priority);
		try {
			factoryBean.afterPropertiesSet();
		} catch (ParseException e) {
			log.error(e.getMessage(), e);
		}
		return factoryBean.getObject();
	}

	/**
	 * Create simple trigger.
	 *
	 * @param triggerName        Trigger name.
	 * @param startTime          Trigger start time.
	 * @param repeatTime         Job repeat period mills
	 * @param misFireInstruction Misfire instruction (what to do in case of misfire
	 *                           happens).
	 * @return {@link SimpleTrigger}
	 */
	public SimpleTrigger createSimpleTrigger(String triggerName, String groupName, Date startTime, Long repeatTime,
			int misFireInstruction, int priority) {
		SimpleTriggerFactoryBean factoryBean = new SimpleTriggerFactoryBean();
		factoryBean.setName(triggerName);
		factoryBean.setGroup(groupName);
		factoryBean.setStartTime(startTime);
		factoryBean.setRepeatCount(1);
		factoryBean.setRepeatInterval(repeatTime);
		factoryBean.setMisfireInstruction(misFireInstruction);
		factoryBean.setPriority(priority);
		factoryBean.afterPropertiesSet();
		return factoryBean.getObject();
	}
}
