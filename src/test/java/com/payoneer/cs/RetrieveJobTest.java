package com.payoneer.cs;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.awaitility.Awaitility;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;

import com.payoneer.cs.helper.TestHelper;
import com.payoneer.cs.job.model.Job;
import com.payoneer.cs.job.model.JobExecutionType;
import com.payoneer.cs.job.model.JobSchedule;
import com.payoneer.cs.job.model.JobStatus;
import com.payoneer.cs.job.model.JobType;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@AutoConfigureWebTestClient
class RetrieveJobTest {

	@Autowired
	private WebTestClient webTestClient;

	@Autowired
	private TestHelper testHelper;

	private CompletableFuture<String> completableFuture = new CompletableFuture<String>();
	private Job job;

	@BeforeEach
	void setup() throws ExecutionException, InterruptedException {
		webTestClient.post().uri("/jms/api/v1/job")
				.body(BodyInserters.fromMultipartData(testHelper.submitJobRequestData().build())).exchange()
				.expectStatus().isCreated().expectHeader()
				.value("location", location -> completableFuture.complete(location));

		String jobId = StringUtils.substringAfter(completableFuture.get(), "/jms/api/v1/job/");

		job = Job.builder().id(jobId).type(JobType.SPRING_BOOT_JAR).status(JobStatus.SUCCESS)
				.fileLocation(".store/" + jobId + ".jar").priority(5).jobGroupName("TestGroupName")
				.jobName("TestPostJob").parameters("TestPostJob").repeatTime(60000l).schedule(new JobSchedule(JobExecutionType.IMMEDIATE, null))
				.build();
	}

	@Test
	void testRetrieveJob() throws ExecutionException, InterruptedException {
		Awaitility.await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofSeconds(20))
				.until(() -> webTestClient.get().uri(completableFuture.get()).exchange().expectStatus().isOk()
						.expectBody(Job.class).returnResult().getResponseBody(), CoreMatchers.equalTo(job));
	}

	@Test
	void testRetrieveAllJob() {
		Awaitility.await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofSeconds(20))
				.until(() -> webTestClient.get().uri("/jms/api/v1/job").exchange().expectStatus().isOk()
						.expectBodyList(Job.class).returnResult().getResponseBody(), CoreMatchers.hasItem(job));

	}
}
