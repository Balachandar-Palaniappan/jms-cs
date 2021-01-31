package com.payoneer.cs.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;

@Configuration
public class AppSwaggerConfig {

	@Bean
	public OpenAPI customOpenAPI() {
		return new OpenAPI().components(new Components())
				.info(new Info().title("Job Management Service")
						.description("This service provides API's for submitting & processing jobs.").version("0.1")
						.contact(new Contact().name("Balachandar Palaniappan").email("balachandar.gct@gmail.com")
								.url("https://www.linkedin.com/in/balachandarp-8379a558/")));
	}
}
