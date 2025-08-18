package com.ai_services.user_management.application.config

import com.ai_services.user_management.infrastructure.security.JwtAuthenticationManager
import com.ai_services.user_management.infrastructure.security.JwtServerSecurityContextRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpMethod
import org.springframework.security.config.annotation.method.configuration.EnableReactiveMethodSecurity
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.security.web.server.SecurityWebFilterChain

@Configuration
@EnableWebFluxSecurity
@EnableReactiveMethodSecurity
class SecurityConfig(
    private val jwtAuthenticationManager: JwtAuthenticationManager,
    private val securityContextRepository: JwtServerSecurityContextRepository
) {
    @Bean
    fun springSecurityFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        return http
            .csrf { it.disable() }
            .cors { }
            .httpBasic { it.disable() }
            .formLogin { it.disable() }
            .securityContextRepository(securityContextRepository)
            .authenticationManager(jwtAuthenticationManager)
            .authorizeExchange { auth ->
                auth
                    .pathMatchers(HttpMethod.POST, "/api/v1/auth/register").permitAll()
                    .pathMatchers(HttpMethod.POST, "/api/v1/auth/login").permitAll()
                    .pathMatchers(HttpMethod.POST, "/api/v1/auth/refresh").permitAll()
                    .pathMatchers("/actuator/**").permitAll()
                    .anyExchange().authenticated()
            }
            .build()
    }

    @Bean
    fun passwordEncoder(): PasswordEncoder = BCryptPasswordEncoder(12)
}


