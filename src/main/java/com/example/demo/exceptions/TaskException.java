package com.example.demo.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Created by as on 07.07.2018.
 */
@ResponseStatus(value = HttpStatus.CONFLICT)
public class TaskException extends RuntimeException{
    public TaskException(String message) {
        super(message);
    }
}
