package com.ai_services.gateway.config

import org.slf4j.LoggerFactory
import com.ai_services.gateway.filters.RateLimitConfig
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver
import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter
import org.springframework.cloud.gateway.route.RouteLocator
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class GatewayRoutes(
    private val ipKeyResolver: KeyResolver,
    private val defaultRedisRateLimiter: RedisRateLimiter,
    private val authRedisRateLimiter: RedisRateLimiter
) {
    private val log = LoggerFactory.getLogger(GatewayRoutes::class.java)
    private val userManagementUriRaw: String = System.getenv("UPSTREAM_USER_MANAGEMENT_URI") ?: "http://user-management:8082"
    private val movieLibraryUriRaw: String = System.getenv("UPSTREAM_MOVIE_LIBRARY_URI") ?: "http://movielibrary:8083"
    private fun sanitize(value: String): String = value.trim().trim('"', '\'', ' ')
    private val userManagementUri: String = runCatching { sanitize(userManagementUriRaw) }.getOrDefault("http://user-management:8082")
    private val movieLibraryUri: String = runCatching { sanitize(movieLibraryUriRaw) }.getOrDefault("http://movielibrary:8083")

    @Bean
    fun customRouteLocator(builder: RouteLocatorBuilder): RouteLocator = builder.routes()
        .also { log.info("Upstreams: userManagementUri={}, movieLibraryUri={}", userManagementUri, movieLibraryUri) }
        // Auth endpoints -> user-management (permitAll in security)
        .route("auth") { r -> r
            .path("/auth/**")
            .filters { f ->
                f.stripPrefix(1)
                f.requestRateLimiter { c ->
                    c.setKeyResolver(ipKeyResolver)
                    c.setRateLimiter(authRedisRateLimiter)
                }
            }
            .uri(userManagementUri)
        }
        // User-management secured endpoints (require JWT)
        .route("user-management-secured") { r -> r
            .path("/users/**", "/mood/**", "/history/**", "/feedback/**")
            .filters { f ->
                f.requestRateLimiter { c ->
                    c.setKeyResolver(ipKeyResolver)
                    c.setRateLimiter(defaultRedisRateLimiter)
                }
            }
            .uri(userManagementUri)
        }
        // Movie-library secured endpoints (require JWT)
        .route("movie-library-secured") { r -> r
            .path("/movies/**")
            .filters { f ->
                f.requestRateLimiter { c ->
                    c.setKeyResolver(ipKeyResolver)
                    c.setRateLimiter(defaultRedisRateLimiter)
                }
            }
            .uri(movieLibraryUri)
        }
        .build()
}


