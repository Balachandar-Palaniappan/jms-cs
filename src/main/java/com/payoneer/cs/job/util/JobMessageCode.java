package com.payoneer.cs.job.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum JobMessageCode {
    MESSAGE_001("Job execution successfully"),
    MESSAGE_002("Job execution failed");

    private String message;
}