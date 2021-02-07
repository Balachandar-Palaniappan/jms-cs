package com.payoneer.cs;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.BodyInserters.MultipartInserter;

import com.payoneer.cs.helper.TestHelper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@DirtiesContext
@AutoConfigureWebTestClient
class SubmitJobTest {

	@Autowired
	private WebTestClient webTestClient;

	@Autowired
	private TestHelper testHelper;

	@Test
	void testSubmitJob() throws InterruptedException {
		MultipartInserter bodyInserters = BodyInserters.fromMultipartData(testHelper.submitJobRequestData().build());
		webTestClient.mutate().responseTimeout(Duration.ofSeconds(30)).build().post().uri("/jms/api/v1/job")
				.body(bodyInserters).exchange().expectStatus().isCreated().returnResult(String.class);
	}

	@Test
	void testSubmitJob_MissingJobFile() {
		webTestClient.post().uri("/jms/api/v1/job")
				.body(BodyInserters.fromMultipartData(testHelper.submitJobRequestData_MissingJobFile().build()))
				.exchange().expectStatus().isBadRequest();

	}

}
