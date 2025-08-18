package com.ai_services.user_management.application.service

import com.ai_services.user_management.domain.feedback.UserFeedback
import com.ai_services.user_management.infrastructure.repository.UserFeedbackRepository
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import reactor.core.publisher.Mono
import java.time.Instant

data class FeedbackRequest(
    @field:NotBlank val feedback_type: String,
    val target_id: String?,
    @field:Min(1) @field:Max(5) val rating: Int?,
    val comment: String?
)

@Service
class FeedbackService(private val repository: UserFeedbackRepository) {
    fun create(userId: String, req: FeedbackRequest): Mono<UserFeedback> =
        repository.save(
            UserFeedback(
                userId = userId,
                feedbackType = req.feedback_type,
                targetId = req.target_id,
                rating = req.rating,
                comment = req.comment,
                createdAt = Instant.now()
            )
        )

    fun list(userId: String): Flux<UserFeedback> = repository.findByUserIdOrderByCreatedAtDesc(userId)
}


