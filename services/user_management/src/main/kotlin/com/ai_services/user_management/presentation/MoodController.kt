package com.ai_services.user_management.presentation

import com.ai_services.user_management.application.service.MoodService
import com.ai_services.user_management.application.service.UpdateMoodRequest
import org.springframework.http.MediaType
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/mood")
class MoodController(private val service: MoodService) {
    @GetMapping("/me")
    fun get(@AuthenticationPrincipal principal: String) = service.get(principal)

    @PostMapping("/me", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun update(@AuthenticationPrincipal principal: String, @RequestBody req: UpdateMoodRequest) = service.update(principal, req)
}


