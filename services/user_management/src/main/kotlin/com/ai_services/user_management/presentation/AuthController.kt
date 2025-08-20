package com.ai_services.user_management.presentation

import com.ai_services.user_management.application.service.AuthService
import com.ai_services.user_management.application.service.LoginRequest
import com.ai_services.user_management.application.service.RegisterRequest
import com.ai_services.user_management.application.service.StartVerificationRequest
import com.ai_services.user_management.application.service.VerifyCodeRequest
import com.ai_services.user_management.infrastructure.security.JwtService
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.http.HttpHeaders
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/api/v1/auth")
class AuthController(private val authService: AuthService, private val jwtService: JwtService) {
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

    // Email verification
    @PostMapping("/start-verification", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun startVerification(@RequestBody req: StartVerificationRequest) = authService.startVerification(req)

    @PostMapping("/verify-code", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun verifyCode(@RequestBody req: VerifyCodeRequest) = authService.verifyCode(req)

    @PostMapping("/resend-code", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun resendCode(@RequestBody req: StartVerificationRequest): Mono<Void> = authService.startVerification(req)

    @PostMapping("/logout-all")
    fun logoutAll(@RequestHeader(HttpHeaders.AUTHORIZATION, required = false) auth: String?): Mono<Void> {
        val token = auth?.takeIf { it.startsWith("Bearer ") }?.removePrefix("Bearer ")
        val userId = token?.let { runCatching { jwtService.parseClaims(it).subject }.getOrNull() }
        return if (userId.isNullOrBlank()) Mono.error(IllegalArgumentException("Unauthorized")) else authService.logoutAll(userId, token)
    }
}


