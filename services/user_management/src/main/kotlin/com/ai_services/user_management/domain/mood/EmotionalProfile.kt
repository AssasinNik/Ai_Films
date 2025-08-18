package com.ai_services.user_management.domain.mood

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.Field
import java.time.Instant

@Document("emotional_profiles")
data class EmotionalProfile(
    @Id val id: String? = null,
    @Field("user_id") val userId: String,
    @Field("current_mood") val currentMood: CurrentMood?,
    @Field("mood_history") val moodHistory: List<MoodHistoryItem> = emptyList(),
    @Field("created_at") val createdAt: Instant = Instant.now(),
    @Field("updated_at") val updatedAt: Instant? = null
)

data class CurrentMood(
    @Field("primary_emotion") val primaryEmotion: String?,
    val intensity: Double?,
    @Field("energy_level") val energyLevel: Double?,
    @Field("stress_level") val stressLevel: Double?
)

data class MoodHistoryItem(
    val date: Instant,
    @Field("primary_emotion") val primaryEmotion: String?,
    val intensity: Double?,
    @Field("energy_level") val energyLevel: Double?,
    val context: String?
)


