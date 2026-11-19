package com.teb.practice.exception;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

import com.teb.practice.response.ApiResponse;

import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ExceptionResponseHandler {

    @ExceptionHandler(OrderNotFoundException.class)
    @ResponseStatus(NOT_FOUND)
    public ApiResponse<String> handleOrderNotFound(OrderNotFoundException e) {

        return new ApiResponse<>(e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(BAD_REQUEST)
    public ApiResponse<String> handleGeneric(IllegalArgumentException e) {

        return new ApiResponse<>(e.getMessage());
    }
}
