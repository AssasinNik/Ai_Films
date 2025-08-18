package com.ai_services.user_management.presentation

import com.ai_services.user_management.application.service.FeedbackRequest
import com.ai_services.user_management.application.service.FeedbackService
import org.springframework.http.MediaType
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/feedback")
class FeedbackController(private val service: FeedbackService) {
    @PostMapping("/me", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun create(@AuthenticationPrincipal principal: String, @RequestBody req: FeedbackRequest) = service.create(principal, req)

    @GetMapping("/me")
    fun list(@AuthenticationPrincipal principal: String) = service.list(principal)
}


