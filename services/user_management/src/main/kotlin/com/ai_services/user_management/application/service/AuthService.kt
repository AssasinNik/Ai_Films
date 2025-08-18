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

data class RegisterRequest(
    @field:Email val email: String,
    @field:Size(min = 8, max = 128) val password: String,
    @field:Size(min = 3, max = 50) val username: String?
)
data class LoginRequest(
    @field:Email val email: String,
    @field:NotBlank val password: String
)
data class AuthResponse(val userId: String, val accessToken: String, val refreshToken: String)

@Service
class AuthService(
    private val userRepository: UserRepository,
    private val passwordEncoder: PasswordEncoder,
    private val jwtService: JwtService,
    private val refreshTokenStore: com.ai_services.user_management.infrastructure.security.RefreshTokenStore
) {
    fun register(request: RegisterRequest): Mono<AuthResponse> {
        val userId = UUID.randomUUID().toString()
        return userRepository.findByEmail(request.email)
            .flatMap<User> { Mono.error(IllegalArgumentException("Email already registered")) }
            .switchIfEmpty(
                Mono.defer {
                    val user = User(
                        userId = userId,
                        email = request.email.lowercase(),
                        username = request.username,
                        passwordHash = passwordEncoder.encode(request.password),
                        createdAt = Instant.now()
                    )
                    userRepository.save(user)
                }
            )
            .cast(User::class.java)
            .flatMap { saved ->
                val tokens = jwtService.generateTokens(saved.userId, mapOf("roles" to listOf("ROLE_USER")))
                val tokenId = tokens.refreshToken.takeLast(16)
                refreshTokenStore.allow(saved.userId, tokenId).thenReturn(
                    AuthResponse(saved.userId, tokens.accessToken, tokens.refreshToken)
                )
            }
    }

    fun login(request: LoginRequest): Mono<AuthResponse> {
        return userRepository.findByEmail(request.email.lowercase())
            .switchIfEmpty(Mono.error(IllegalArgumentException("Invalid credentials")))
            .flatMap { user ->
                if (!passwordEncoder.matches(request.password, user.passwordHash)) {
                    Mono.error(IllegalArgumentException("Invalid credentials"))
                } else {
                    val tokens = jwtService.generateTokens(user.userId, mapOf("roles" to listOf("ROLE_USER")))
                    val tokenId = tokens.refreshToken.takeLast(16)
                    refreshTokenStore.allow(user.userId, tokenId).thenReturn(
                        AuthResponse(user.userId, tokens.accessToken, tokens.refreshToken)
                    )
                }
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
}


