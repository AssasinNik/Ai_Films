package com.ai_services.user_management.presentation

import com.ai_services.user_management.application.service.AddHistoryRequest
import com.ai_services.user_management.application.service.HistoryService
import org.springframework.http.MediaType
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/history")
class HistoryController(private val service: HistoryService) {
    @PostMapping("/me", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun add(@AuthenticationPrincipal principal: String, @RequestBody req: AddHistoryRequest) = service.add(principal, req)

    @GetMapping("/me")
    fun list(@AuthenticationPrincipal principal: String) = service.list(principal)
}


