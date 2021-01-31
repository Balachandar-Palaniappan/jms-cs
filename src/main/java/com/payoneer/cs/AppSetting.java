package com.payoneer.cs;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;

@Component
@ConfigurationProperties("app")
@Getter
@Setter
public class AppSetting {
	private String jobStore;
}
