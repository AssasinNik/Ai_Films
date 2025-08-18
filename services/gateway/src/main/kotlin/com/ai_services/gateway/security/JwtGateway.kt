package com.ai_services.gateway.security

import io.jsonwebtoken.Claims
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.io.Decoders
import io.jsonwebtoken.security.Keys
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.web.server.SecurityWebFilterChain
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import org.springframework.stereotype.Component
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.publisher.Mono
import java.nio.charset.StandardCharsets
import java.security.Key
import java.security.MessageDigest

@Component
class JwtVerifier(
    @Value("\${security.jwt.secret}") private val secret: String,
    @Value("\${security.jwt.issuer}") private val issuer: String,
) {
    private fun key(): Key {
        val bytes = try { Decoders.BASE64.decode(secret) } catch (_: Exception) { secret.toByteArray(StandardCharsets.UTF_8) }
        val material = if (bytes.size < 32) MessageDigest.getInstance("SHA-256").digest(bytes) else bytes
        return Keys.hmacShaKeyFor(material)
    }

    fun validate(token: String): Claims? = try {
        val claims = Jwts.parserBuilder().setSigningKey(key()).build().parseClaimsJws(token).body
        if (claims.issuer != issuer) null else claims
    } catch (e: Exception) { null }
}

@Configuration
@EnableWebFluxSecurity
class GatewaySecurity(private val verifier: JwtVerifier) {
    @Bean
    fun springSecurityFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain =
        http
            .csrf { it.disable() }
            .cors { }
            .authorizeExchange { auth ->
                auth.anyExchange().permitAll()
            }
            .build()
}


