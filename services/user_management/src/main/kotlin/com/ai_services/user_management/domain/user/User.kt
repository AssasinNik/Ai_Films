package com.ai_services.user_management.domain.user

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.Field
import java.time.Instant

@Document("users")
data class User(
    @Id val id: String? = null,
    @Indexed(unique = true) @Field("user_id") val userId: String,
    @Indexed(unique = true) val email: String,
    val username: String?,
    @Field("password_hash") val passwordHash: String,
    @Field("photo_url") val photoUrl: String? = null,
    @Field("created_at") val createdAt: Instant = Instant.now(),
    @Field("updated_at") val updatedAt: Instant? = null,
    @Field("last_active") val lastActive: Instant? = null
)


