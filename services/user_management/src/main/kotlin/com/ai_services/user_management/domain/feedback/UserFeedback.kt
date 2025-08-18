package com.ai_services.user_management.domain.feedback

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.Field
import java.time.Instant

@Document("user_feedback")
data class UserFeedback(
    @Id val id: String? = null,
    @Field("user_id") val userId: String,
    @Field("feedback_type") val feedbackType: String,
    @Field("target_id") val targetId: String?,
    val rating: Int?,
    val comment: String?,
    @Field("created_at") val createdAt: Instant = Instant.now()
)


