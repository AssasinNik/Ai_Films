package com.ai_services.user_management.infrastructure.security

import io.jsonwebtoken.ExpiredJwtException
import org.springframework.http.HttpHeaders
import org.springframework.security.authentication.AbstractAuthenticationToken
import org.springframework.security.authentication.ReactiveAuthenticationManager
import org.springframework.security.core.Authentication
import org.springframework.security.core.GrantedAuthority
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContext
import org.springframework.security.core.context.SecurityContextImpl
import org.springframework.security.web.server.context.ServerSecurityContextRepository
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono

class JwtAuthenticationToken(
    private val principalName: String,
    private val authoritiesList: Collection<GrantedAuthority>
) : AbstractAuthenticationToken(authoritiesList) {
    init { super.setAuthenticated(true) }
    override fun getCredentials(): Any = "N/A"
    override fun getPrincipal(): Any = principalName
}

@Component
class JwtAuthenticationManager(private val jwtService: JwtService) : ReactiveAuthenticationManager {
    override fun authenticate(authentication: Authentication): Mono<Authentication> {
        return Mono.fromCallable {
            val token = authentication.credentials as String
            val claims = jwtService.parseClaims(token)
            val subject = claims.subject
            val roles = (claims["roles"] as? Collection<*>)?.map { SimpleGrantedAuthority(it.toString()) } ?: emptyList()
            JwtAuthenticationToken(subject, roles) as Authentication
        }.onErrorResume(ExpiredJwtException::class.java) { Mono.empty() }
    }
}

@Component
class JwtServerSecurityContextRepository(private val authManager: JwtAuthenticationManager) : ServerSecurityContextRepository {
    override fun save(exchange: ServerWebExchange, context: SecurityContext): Mono<Void> = Mono.empty()

    override fun load(exchange: ServerWebExchange): Mono<SecurityContext> {
        val authHeader = exchange.request.headers.getFirst(HttpHeaders.AUTHORIZATION) ?: return Mono.empty()
        if (!authHeader.startsWith("Bearer ")) return Mono.empty()
        val authToken = authHeader.substring(7)
        val auth = object : Authentication {
            private var authenticated = false
            override fun getAuthorities(): MutableCollection<out GrantedAuthority> = mutableListOf()
            override fun setAuthenticated(isAuthenticated: Boolean) { authenticated = isAuthenticated }
            override fun getName(): String = "jwt"
            override fun getCredentials(): Any = authToken
            override fun getDetails(): Any? = null
            override fun getPrincipal(): Any = "jwt"
            override fun isAuthenticated(): Boolean = authenticated
        }
        return authManager.authenticate(auth).map { SecurityContextImpl(it) }
    }
}


