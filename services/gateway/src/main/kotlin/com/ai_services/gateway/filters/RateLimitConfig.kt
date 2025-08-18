package com.ai_services.gateway.filters

import org.springframework.beans.factory.annotation.Value
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver
import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import reactor.core.publisher.Mono

@Configuration
class RateLimitConfig {
    // Resolve key by client IP; can be extended per route
    @Bean
    fun ipKeyResolver(): KeyResolver = KeyResolver { exchange ->
        val ip = exchange.request.headers.getFirst("X-Forwarded-For")
            ?: exchange.request.remoteAddress?.address?.hostAddress
            ?: "unknown"
        Mono.just(ip)
    }

    // Default Redis rate limiter: replenishRate/burstCapacity
    @Bean
    @Primary
    fun defaultRedisRateLimiter(
        @Value("\${gateway.rate.replenish:100}") replenish: Int,
        @Value("\${gateway.rate.burst:200}") burst: Int
    ): RedisRateLimiter = RedisRateLimiter(replenish, burst)

    // Stricter limiter for auth endpoints
    @Bean
    fun authRedisRateLimiter(
        @Value("\${gateway.rate.auth.replenish:20}") replenish: Int,
        @Value("\${gateway.rate.auth.burst:40}") burst: Int
    ): RedisRateLimiter = RedisRateLimiter(replenish, burst)
}


