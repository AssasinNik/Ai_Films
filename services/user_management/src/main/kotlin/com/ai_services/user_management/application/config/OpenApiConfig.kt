package com.ai_services.user_management.application.config

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.info.License
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenApiConfig {
    @Bean
    fun openAPI(): OpenAPI = OpenAPI()
        .info(
            Info()
                .title("User Management API")
                .description("API управления пользователями и их эмоциональными профилями")
                .version("v1")
                .license(License().name("Apache 2.0").url("https://www.apache.org/licenses/LICENSE-2.0"))
        )
}


