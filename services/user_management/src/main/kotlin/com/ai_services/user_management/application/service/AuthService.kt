package com.ai_services.user_management.application.service

import com.ai_services.user_management.domain.user.User
import com.ai_services.user_management.infrastructure.repository.UserRepository
import com.ai_services.user_management.infrastructure.security.JwtService
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.UUID

import jakarta.validation.constraints.Email
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

data class RegisterRequest(
    @field:Email val email: String,
    @field:Size(min = 8, max = 128) val password: String,
    @field:Size(min = 3, max = 50) val username: String?
)
data class LoginRequest(
    @field:Email val email: String,
    @field:NotBlank val password: String
)
data class AuthResponse(val userId: String, val accessToken: String?, val refreshToken: String?, val requiresVerification: Boolean = false)
data class StartVerificationRequest(val email: String)
data class VerifyCodeRequest(val email: String, val code: String)

@Service
class AuthService(
    private val userRepository: UserRepository,
    private val passwordEncoder: PasswordEncoder,
    private val jwtService: JwtService,
    private val refreshTokenStore: com.ai_services.user_management.infrastructure.security.RefreshTokenStore,
    private val emailService: EmailService,
    private val redisTemplate: org.springframework.data.redis.core.ReactiveStringRedisTemplate
) {
    fun register(request: RegisterRequest): Mono<AuthResponse> {
        val email = request.email.lowercase()
        val objectMapper = jacksonObjectMapper()
        val regKey = "verify:register:$email"
        val pending = mapOf(
            "username" to (request.username ?: ""),
            "passwordHash" to passwordEncoder.encode(request.password)
        )
        return userRepository.findByEmail(email)
            .flatMap<User> { Mono.error(IllegalArgumentException("Email already registered")) }
            .switchIfEmpty(
                Mono.defer {
                    val json = objectMapper.writeValueAsString(pending)
                    redisTemplate.opsForValue().set(regKey, json, java.time.Duration.ofMinutes(10))
                        .then(Mono.empty())
                }
            )
            .then(startVerification(StartVerificationRequest(email)))
            .thenReturn(AuthResponse(userId = "", accessToken = null, refreshToken = null, requiresVerification = true))
    }

    fun login(request: LoginRequest): Mono<AuthResponse> =
        userRepository.findByEmail(request.email.lowercase())
            .switchIfEmpty(Mono.error(IllegalArgumentException("Invalid credentials")))
            .flatMap { user ->
                if (!passwordEncoder.matches(request.password, user.passwordHash)) {
                    Mono.error(IllegalArgumentException("Invalid credentials"))
                } else {
                    startVerification(StartVerificationRequest(user.email)).thenReturn(
                        AuthResponse(user.userId, null, null, requiresVerification = true)
                    )
                }
            }

    fun refresh(refreshToken: String): Mono<AuthResponse> {
        return Mono.fromCallable {
            val claims = jwtService.parseClaims(refreshToken)
            if (claims["type"] != "refresh") throw IllegalArgumentException("Invalid refresh token")
            claims.subject
        }.flatMap { userId ->
            refreshTokenStore.isBlacklisted(refreshToken).flatMap { isBl ->
                if (isBl) Mono.error(IllegalArgumentException("Token blacklisted")) else Mono.just(userId)
            }
        }.flatMap { userId ->
            val tokenId = refreshToken.takeLast(16)
            refreshTokenStore.isAllowed(userId, tokenId).flatMap { allowed ->
                if (!allowed) Mono.error(IllegalArgumentException("Token not allowed")) else Mono.just(userId)
            }
        }.flatMap { userId ->
            userRepository.findByUserId(userId)
                .switchIfEmpty(Mono.error(IllegalArgumentException("User not found")))
                .flatMap { user ->
                    // ротация: старый refresh -> блэклист, новый -> allow
                    val newTokens = jwtService.generateTokens(user.userId, mapOf("roles" to listOf("ROLE_USER")))
                    val newTokenId = newTokens.refreshToken.takeLast(16)
                    refreshTokenStore.blacklist(refreshToken)
                        .then(refreshTokenStore.allow(user.userId, newTokenId))
                        .thenReturn(AuthResponse(user.userId, newTokens.accessToken, newTokens.refreshToken))
                }
        }
    }

    fun logoutAll(userId: String, refreshToken: String?): Mono<Void> {
        val blacklistStep = if (!refreshToken.isNullOrBlank()) refreshTokenStore.blacklist(refreshToken) else Mono.just(true)
        return blacklistStep.then(refreshTokenStore.revokeAll(userId)).then()
    }

    // --- Email verification ---
    private fun verificationKey(email: String) = "verify:code:${email.lowercase()}"

    fun startVerification(req: StartVerificationRequest): Mono<Void> {
        val code = (100000..999999).random().toString()
        val key = verificationKey(req.email)
        // атомарно: устанавливаем код и TTL за одну операцию
        return redisTemplate.opsForValue()
            .set(key, code, java.time.Duration.ofMinutes(10))
            .then(Mono.fromRunnable { emailService.sendVerificationCode(req.email, "Код подтверждения AI Films", code) })
    }

    fun verifyCode(req: VerifyCodeRequest): Mono<AuthResponse> {
        val email = req.email.lowercase()
        val objectMapper = jacksonObjectMapper()
        val regKey = "verify:register:$email"
        return redisTemplate.opsForValue().get(verificationKey(email)).flatMap { saved ->
            if (saved == req.code) {
                userRepository.findByEmail(email).flatMap<AuthResponse> { user ->
                    // Пользователь существует (логин-флоу)
                    val tokens = jwtService.generateTokens(user.userId, mapOf("roles" to listOf("ROLE_USER")))
                    val tokenId = tokens.refreshToken.takeLast(16)
                    refreshTokenStore.allow(user.userId, tokenId)
                        .then(redisTemplate.delete(verificationKey(email)))
                        .thenReturn(AuthResponse(user.userId, tokens.accessToken, tokens.refreshToken))
                }.switchIfEmpty(
                    // Регистрация завершается здесь: создаём пользователя из pending
                    redisTemplate.opsForValue().get(regKey).flatMap { json ->
                        if (json.isNullOrBlank()) Mono.error(IllegalStateException("No pending registration")) else Mono.just(json)
                    }.flatMap { json ->
                        val pending: Map<String, String> = objectMapper.readValue(json)
                        val userId = UUID.randomUUID().toString()
                        val user = User(
                            userId = userId,
                            email = email,
                            username = pending["username"],
                            passwordHash = pending["passwordHash"] ?: "",
                            createdAt = Instant.now()
                        )
                        userRepository.save(user).flatMap { saved ->
                            val tokens = jwtService.generateTokens(saved.userId, mapOf("roles" to listOf("ROLE_USER")))
                            val tokenId = tokens.refreshToken.takeLast(16)
                            refreshTokenStore.allow(saved.userId, tokenId)
                                .then(redisTemplate.delete(verificationKey(email)))
                                .then(redisTemplate.delete(regKey))
                                .thenReturn(AuthResponse(saved.userId, tokens.accessToken, tokens.refreshToken))
                        }
                    }
                )
            } else Mono.error(IllegalArgumentException("Invalid verification code"))
        }
    }
}


