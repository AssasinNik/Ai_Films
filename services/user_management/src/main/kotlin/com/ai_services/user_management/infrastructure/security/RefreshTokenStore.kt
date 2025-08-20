package com.ai_services.user_management.infrastructure.security

import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.time.Duration

@Component
class RefreshTokenStore(
    private val redis: ReactiveStringRedisTemplate,
    @Value("\${security.jwt.refresh-token-ttl}") private val refreshTtl: java.time.Duration
) {
    private fun key(userId: String, tokenId: String) = "refresh:allow:$userId:$tokenId"
    private fun blKey(token: String) = "refresh:blacklist:$token"
    private fun revokeAllKey(userId: String) = "refresh:revoke_all:$userId"

    fun allow(userId: String, tokenId: String): Mono<Boolean> =
        redis.opsForValue().set(key(userId, tokenId), "1", Duration.ofSeconds(refreshTtl.seconds))

    fun isAllowed(userId: String, tokenId: String): Mono<Boolean> {
        val checkAllowed = redis.opsForValue().get(key(userId, tokenId)).map { it == "1" }.defaultIfEmpty(false)
        return redis.opsForValue().get(revokeAllKey(userId))
            .flatMap { revoked -> if (revoked == "1") Mono.just(false) else checkAllowed }
            .switchIfEmpty(checkAllowed)
    }

    fun blacklist(token: String): Mono<Boolean> =
        redis.opsForValue().set(blKey(token), "1", Duration.ofSeconds(refreshTtl.seconds))

    fun isBlacklisted(token: String): Mono<Boolean> =
        redis.opsForValue().get(blKey(token)).map { it == "1" }.defaultIfEmpty(false)

    fun revokeAll(userId: String): Mono<Boolean> =
        redis.opsForValue().set(revokeAllKey(userId), "1", Duration.ofSeconds(refreshTtl.seconds))
}


