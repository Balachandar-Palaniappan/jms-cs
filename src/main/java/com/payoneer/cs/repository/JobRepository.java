package com.payoneer.cs.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.payoneer.cs.job.model.JobData;

@Repository
public interface JobRepository extends JpaRepository<JobData, String> {

}