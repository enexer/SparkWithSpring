package com.example.demo.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Created by as on 07.07.2018.
 */
@ResponseStatus(value = HttpStatus.NOT_FOUND)
public class TaskNotFoundException extends RuntimeException {

    private Object domain;

    public TaskNotFoundException(Class<? extends Object> domainClass) {
        super("Object with type " + domainClass.getSimpleName() + " was not found!");
        this.domain = domainClass;
    }

    public TaskNotFoundException(String message) {
        super(message);
    }
}