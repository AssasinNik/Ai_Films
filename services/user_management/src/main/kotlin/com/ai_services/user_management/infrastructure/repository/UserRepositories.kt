package com.ai_services.user_management.infrastructure.repository

import com.ai_services.user_management.domain.feedback.UserFeedback
import com.ai_services.user_management.domain.history.ViewingHistory
import com.ai_services.user_management.domain.mood.EmotionalProfile
import com.ai_services.user_management.domain.user.User
import org.springframework.data.mongodb.repository.ReactiveMongoRepository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface UserRepository : ReactiveMongoRepository<User, String> {
    fun findByEmail(email: String): Mono<User>
    fun findByUserId(userId: String): Mono<User>
}

interface EmotionalProfileRepository : ReactiveMongoRepository<EmotionalProfile, String> {
    fun findByUserId(userId: String): Mono<EmotionalProfile>
}

interface ViewingHistoryRepository : ReactiveMongoRepository<ViewingHistory, String> {
    fun findByUserIdOrderByCreatedAtDesc(userId: String): Flux<ViewingHistory>
}

interface UserFeedbackRepository : ReactiveMongoRepository<UserFeedback, String> {
    fun findByUserIdOrderByCreatedAtDesc(userId: String): Flux<UserFeedback>
}


