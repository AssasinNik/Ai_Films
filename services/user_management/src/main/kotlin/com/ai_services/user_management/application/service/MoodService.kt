package com.ai_services.user_management.application.service

import com.ai_services.user_management.domain.mood.CurrentMood
import com.ai_services.user_management.domain.mood.EmotionalProfile
import com.ai_services.user_management.domain.mood.MoodHistoryItem
import com.ai_services.user_management.infrastructure.repository.EmotionalProfileRepository
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.Size
import java.time.Instant

data class UpdateMoodRequest(
    @field:Size(min = 2, max = 30) val primary_emotion: String?,
    @field:Min(0) @field:Max(1) val intensity: Double?,
    @field:Min(0) @field:Max(1) val energy_level: Double?,
    @field:Min(0) @field:Max(1) val stress_level: Double?
)

@Service
class MoodService(private val repository: EmotionalProfileRepository) {
    fun get(userId: String): Mono<EmotionalProfile> =
        repository.findByUserId(userId)
            .switchIfEmpty(
                repository.save(
                    EmotionalProfile(
                        userId = userId,
                        currentMood = null,
                        moodHistory = emptyList(),
                        createdAt = Instant.now()
                    )
                )
            )

    fun update(userId: String, req: UpdateMoodRequest): Mono<EmotionalProfile> =
        get(userId).flatMap { profile ->
            val now = Instant.now()
            val newCurrent = CurrentMood(
                primaryEmotion = req.primary_emotion,
                intensity = req.intensity,
                energyLevel = req.energy_level,
                stressLevel = req.stress_level
            )
            val historyItem = MoodHistoryItem(
                date = now,
                primaryEmotion = req.primary_emotion,
                intensity = req.intensity,
                energyLevel = req.energy_level,
                context = null
            )
            repository.save(
                profile.copy(
                    currentMood = newCurrent,
                    moodHistory = listOf(historyItem) + profile.moodHistory,
                    updatedAt = now
                )
            )
        }
}


