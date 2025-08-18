package com.ai_services.user_management.infrastructure.security

import io.jsonwebtoken.Claims
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.SignatureAlgorithm
import io.jsonwebtoken.io.Decoders
import io.jsonwebtoken.security.Keys
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.security.Key
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.util.Date

data class JwtTokens(
    val accessToken: String,
    val refreshToken: String,
    val accessTokenExpiresAt: Instant,
    val refreshTokenExpiresAt: Instant
)

@Component
class JwtService(
    @Value("\${security.jwt.secret}") private val secret: String,
    @Value("\${security.jwt.issuer}") private val issuer: String,
    @Value("\${security.jwt.access-token-ttl}") private val accessTtl: Duration,
    @Value("\${security.jwt.refresh-token-ttl}") private val refreshTtl: Duration
) {
    private val log = LoggerFactory.getLogger(JwtService::class.java)

    private fun buildSigningKey(): Key {
        val keyBytes: ByteArray = try {
            val decoded = Decoders.BASE64.decode(secret)
            if (decoded.size < 32) MessageDigest.getInstance("SHA-256").digest(decoded) else decoded
        } catch (e: Exception) {
            // secret не в base64 — используем UTF-8 байты, при необходимости доводим до 256 бит через SHA-256
            val raw = secret.toByteArray(StandardCharsets.UTF_8)
            if (raw.size < 32) MessageDigest.getInstance("SHA-256").digest(raw) else raw
        }
        return Keys.hmacShaKeyFor(keyBytes)
    }

    private val signingKey: Key by lazy { buildSigningKey() }

    fun generateTokens(subject: String, additionalClaims: Map<String, Any> = emptyMap()): JwtTokens {
        val now = Instant.now()
        val accessExp = now.plus(accessTtl)
        val refreshExp = now.plus(refreshTtl)

        val access = Jwts.builder()
            .setSubject(subject)
            .setIssuer(issuer)
            .setIssuedAt(Date.from(now))
            .setExpiration(Date.from(accessExp))
            .addClaims(additionalClaims)
            .signWith(signingKey, SignatureAlgorithm.HS256)
            .compact()

        val refresh = Jwts.builder()
            .setSubject(subject)
            .setIssuer(issuer)
            .setIssuedAt(Date.from(now))
            .setExpiration(Date.from(refreshExp))
            .claim("type", "refresh")
            .signWith(signingKey, SignatureAlgorithm.HS256)
            .compact()

        log.debug("Generated tokens for subject={} accessExp={} refreshExp={}", subject, accessExp, refreshExp)
        return JwtTokens(access, refresh, accessExp, refreshExp)
    }

    fun parseClaims(jwt: String): Claims =
        Jwts.parserBuilder().setSigningKey(signingKey).build().parseClaimsJws(jwt).body
}


