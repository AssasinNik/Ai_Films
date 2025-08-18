package com.ai_services.user_management.presentation

import com.ai_services.user_management.application.service.AuthService
import com.ai_services.user_management.application.service.LoginRequest
import com.ai_services.user_management.application.service.RegisterRequest
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/api/v1/auth")
class AuthController(private val authService: AuthService) {
    private val log = LoggerFactory.getLogger(AuthController::class.java)

    @PostMapping("/register", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun register(@RequestBody @Validated request: RegisterRequest) =
        authService.register(request).doOnSubscribe { log.info("Register attempt email={}", request.email) }

    @PostMapping("/login", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun login(@RequestBody @Validated request: LoginRequest) =
        authService.login(request).doOnSubscribe { log.info("Login attempt email={}", request.email) }

    data class RefreshRequest(@field:jakarta.validation.constraints.NotBlank val refreshToken: String)

    @PostMapping("/refresh", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun refresh(@RequestBody request: RefreshRequest) =
        authService.refresh(request.refreshToken).doOnSubscribe { log.info("Refresh attempt") }
}


