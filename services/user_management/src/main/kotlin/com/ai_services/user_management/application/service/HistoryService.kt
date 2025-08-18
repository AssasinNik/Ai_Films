package com.ai_services.user_management.application.service

import com.ai_services.user_management.domain.history.ViewingHistory
import com.ai_services.user_management.domain.history.ViewingInfo
import com.ai_services.user_management.infrastructure.repository.ViewingHistoryRepository
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import reactor.core.publisher.Mono
import java.time.Instant

data class AddHistoryRequest(
    @field:NotBlank val movie_id: String,
    val start_time: Instant,
    val end_time: Instant?,
    @field:Min(0) val duration_watched: Int,
    @field:Min(0) @field:Max(100) val completion_percentage: Int,
    val device_type: String?,
    val quality: String?
)

@Service
class HistoryService(private val repository: ViewingHistoryRepository) {
    fun add(userId: String, req: AddHistoryRequest): Mono<ViewingHistory> {
        val info = ViewingInfo(
            startTime = req.start_time,
            endTime = req.end_time,
            durationWatched = req.duration_watched,
            completionPercentage = req.completion_percentage,
            deviceType = req.device_type,
            quality = req.quality
        )
        return repository.save(
            ViewingHistory(
                userId = userId,
                movieId = req.movie_id,
                sessionId = null,
                viewingInfo = info,
                context = null,
                userInteraction = null,
                emotionalResponse = null,
                createdAt = Instant.now()
            )
        )
    }

    fun list(userId: String): Flux<ViewingHistory> = repository.findByUserIdOrderByCreatedAtDesc(userId)
}


