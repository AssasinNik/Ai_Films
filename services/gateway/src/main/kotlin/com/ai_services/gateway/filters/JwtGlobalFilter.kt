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
    private val publicPrefixes = listOf("/actuator", "/openapi", "/swagger-ui", "/v3/api-docs", "/auth/")

    override fun filter(exchange: ServerWebExchange, chain: org.springframework.cloud.gateway.filter.GatewayFilterChain): Mono<Void> {
        val path = exchange.request.path.toString()
        if (publicPrefixes.any { path.startsWith(it) }) {
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
        return chain.filter(exchange)
    }

    override fun getOrder(): Int = -50
}


