package com.ai_services.user_management.domain.history

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.Field
import java.time.Instant

@Document("viewing_history")
data class ViewingHistory(
    @Id val id: String? = null,
    @Field("user_id") val userId: String,
    @Field("movie_id") val movieId: String,
    @Field("session_id") val sessionId: String?,
    @Field("viewing_info") val viewingInfo: ViewingInfo,
    val context: ViewingContext?,
    @Field("user_interaction") val userInteraction: UserInteraction?,
    @Field("emotional_response") val emotionalResponse: EmotionalResponse?,
    @Field("created_at") val createdAt: Instant = Instant.now()
)

data class ViewingInfo(
    @Field("start_time") val startTime: Instant,
    @Field("end_time") val endTime: Instant?,
    @Field("duration_watched") val durationWatched: Int,
    @Field("completion_percentage") val completionPercentage: Int,
    @Field("device_type") val deviceType: String?,
    val quality: String?
)

data class ViewingContext(
    @Field("time_of_day") val timeOfDay: String?,
    @Field("day_of_week") val dayOfWeek: String?,
    @Field("viewing_alone") val viewingAlone: Boolean?,
    @Field("planned_viewing") val plannedViewing: Boolean?,
    @Field("recommendation_source") val recommendationSource: String?
)

data class UserInteraction(
    @Field("rating_given") val ratingGiven: Int?,
    @Field("rating_timestamp") val ratingTimestamp: Instant?,
    @Field("review_written") val reviewWritten: Boolean?,
    @Field("added_to_favorites") val addedToFavorites: Boolean?,
    val rewatched: Boolean?
)

data class EmotionalResponse(
    @Field("overall_satisfaction") val overallSatisfaction: Double?,
    @Field("emotional_impact") val emotionalImpact: String?,
    @Field("mood_change") val moodChange: Double?,
    @Field("would_recommend") val wouldRecommend: Boolean?,
    @Field("mood_match") val moodMatch: Double?
)


