package com.ai_services.gateway.filters

import org.slf4j.LoggerFactory
import org.springframework.cloud.gateway.filter.GlobalFilter
import org.springframework.core.Ordered
import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.util.UUID

@Component
class RequestIdTraceFilter : GlobalFilter, Ordered {
    private val log = LoggerFactory.getLogger(RequestIdTraceFilter::class.java)

    override fun filter(exchange: org.springframework.web.server.ServerWebExchange, chain: org.springframework.cloud.gateway.filter.GatewayFilterChain): Mono<Void> {
        val request: ServerHttpRequest = exchange.request
        val reqId = request.headers.getFirst("X-Request-Id") ?: UUID.randomUUID().toString()
        val newRequest = request.mutate()
            .header("X-Request-Id", reqId)
            .build()
        val mutated = exchange.mutate().request(newRequest).build()
        mutated.response.headers.add("X-Request-Id", reqId)
        return chain.filter(mutated)
    }

    override fun getOrder(): Int = -100
}


