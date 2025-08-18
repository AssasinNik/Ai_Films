package com.ai_services.user_management.infrastructure.rate_limit

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono
import java.time.Duration

@Component
class RateLimitFilter(
    private val redis: ReactiveStringRedisTemplate,
    @Value("\${rate.limit.requests:100}") private val maxRequests: Long,
    @Value("\${rate.limit.window-seconds:60}") private val windowSeconds: Long
) : WebFilter {
    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val path = exchange.request.path.toString()
        // Больше ограничиваем аутентификационные эндпоинты
        val weight = if (path.startsWith("/api/v1/auth")) 5 else 1
        val clientKey = exchange.request.headers.getFirst("X-Forwarded-For")
            ?: exchange.request.remoteAddress?.address?.hostAddress
            ?: "unknown"
        val key = "ratelimit:${clientKey}:${windowSeconds}:${path}"
        val expire = Duration.ofSeconds(windowSeconds)

        return redis.opsForValue().increment(key, weight.toLong())
            .flatMap { count ->
                redis.expire(key, expire).thenReturn(count)
            }
            .flatMap { count ->
                if (count > maxRequests) {
                    exchange.response.statusCode = HttpStatus.TOO_MANY_REQUESTS
                    val headers = exchange.response.headers
                    headers.add("Retry-After", windowSeconds.toString())
                    exchange.response.setComplete()
                } else {
                    chain.filter(exchange)
                }
            }
    }
}

// Bean создается через @Component выше; конфигурация по умолчанию задается через application.properties


