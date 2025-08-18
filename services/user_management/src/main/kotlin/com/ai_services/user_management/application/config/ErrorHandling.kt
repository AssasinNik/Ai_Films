package com.ai_services.user_management.application.config

import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.support.WebExchangeBindException
import org.springframework.web.server.ServerWebInputException
import reactor.core.publisher.Mono

data class ErrorResponse(val status: Int, val error: String, val message: String?)

@ControllerAdvice
class GlobalErrorHandler {
    private val log = LoggerFactory.getLogger(GlobalErrorHandler::class.java)

    @ExceptionHandler(IllegalArgumentException::class)
    fun handleIllegalArgument(ex: IllegalArgumentException): Mono<org.springframework.http.ResponseEntity<ErrorResponse>> =
        Mono.fromCallable {
            log.warn("Bad request: {}", ex.message, ex)
            org.springframework.http.ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .contentType(MediaType.APPLICATION_JSON)
                .body(ErrorResponse(400, "Bad Request", ex.message))
        }

    @ExceptionHandler(NoSuchElementException::class)
    fun handleNotFound(ex: NoSuchElementException): Mono<org.springframework.http.ResponseEntity<ErrorResponse>> =
        Mono.fromCallable {
            log.warn("Not found: {}", ex.message, ex)
            org.springframework.http.ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .contentType(MediaType.APPLICATION_JSON)
                .body(ErrorResponse(404, "Not Found", ex.message))
        }

    @ExceptionHandler(value = [WebExchangeBindException::class, MethodArgumentNotValidException::class, ServerWebInputException::class])
    fun handleValidation(ex: Exception): Mono<org.springframework.http.ResponseEntity<ErrorResponse>> =
        Mono.fromCallable {
            log.warn("Validation failed: {}", ex.message, ex)
            org.springframework.http.ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .contentType(MediaType.APPLICATION_JSON)
                .body(ErrorResponse(400, "Validation Failed", ex.message))
        }

    @ExceptionHandler(Exception::class)
    fun handleGeneric(ex: Exception): Mono<org.springframework.http.ResponseEntity<ErrorResponse>> =
        Mono.fromCallable {
            log.error("Unhandled error: {}", ex.message, ex)
            org.springframework.http.ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .contentType(MediaType.APPLICATION_JSON)
                .body(ErrorResponse(500, "Internal Server Error", ex.message))
        }
}


