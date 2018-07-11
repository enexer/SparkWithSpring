package com.example.demo.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Created by as on 09.07.2018.
 */
@ResponseStatus(value = HttpStatus.CONFLICT)
public class SparkMasterUrlException  extends RuntimeException {
    public SparkMasterUrlException(String message) {
        super(message);
    }
}
