package com.ai_services.gateway.filters

import com.ai_services.gateway.security.JwtVerifier
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.cloud.gateway.filter.GlobalFilter
import org.springframework.core.Ordered
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono

@Component
class JwtGlobalFilter(private val verifier: JwtVerifier) : GlobalFilter, Ordered {
    private val publicPrefixes = listOf("/actuator", "/openapi", "/swagger-ui", "/v3/api-docs")

    private fun isPublic(path: String): Boolean {
        if (publicPrefixes.any { path.startsWith(it) }) return true
        // allow only register/login/refresh + start-verification/verify-code/resend-code under auth
        return path == "/auth/api/v1/auth/register" ||
               path == "/auth/api/v1/auth/login" ||
               path == "/auth/api/v1/auth/refresh" ||
               path == "/auth/api/v1/auth/start-verification" ||
               path == "/auth/api/v1/auth/verify-code" ||
               path == "/auth/api/v1/auth/resend-code"
    }

    override fun filter(exchange: ServerWebExchange, chain: org.springframework.cloud.gateway.filter.GatewayFilterChain): Mono<Void> {
        val path = exchange.request.path.toString()
        if (isPublic(path)) {
            return chain.filter(exchange)
        }
        val hdr = exchange.request.headers.getFirst(HttpHeaders.AUTHORIZATION)
        if (hdr == null || !hdr.startsWith("Bearer ")) {
            exchange.response.statusCode = HttpStatus.UNAUTHORIZED
            return exchange.response.setComplete()
        }
        val token = hdr.substring(7)
        val claims = verifier.validate(token)
        if (claims == null) {
            exchange.response.statusCode = HttpStatus.UNAUTHORIZED
            return exchange.response.setComplete()
        }
        val mutated = exchange.mutate().request(
            exchange.request.mutate()
                .header("X-User-Id", claims.subject ?: "")
                .build()
        ).build()
        return chain.filter(mutated)
    }

    override fun getOrder(): Int = -50
}


