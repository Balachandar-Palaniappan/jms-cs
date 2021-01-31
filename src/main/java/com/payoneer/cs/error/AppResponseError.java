package com.payoneer.cs.error;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AppResponseError {
    private String message;
}