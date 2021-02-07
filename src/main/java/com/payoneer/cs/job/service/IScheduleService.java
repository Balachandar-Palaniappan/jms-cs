package com.payoneer.cs.job.service;

import java.util.List;
import java.util.Optional;

import com.payoneer.cs.job.model.Job;

public interface IScheduleService {
	String submitJob(Job job);

	List<Job> getAllJobs();

	Optional<Job> getJobById(String id);

	boolean deleteJob(Job job);

	boolean updateScheduleJob(Job job);
}
