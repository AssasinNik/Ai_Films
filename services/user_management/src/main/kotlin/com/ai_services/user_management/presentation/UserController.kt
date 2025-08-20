package com.ai_services.user_management.presentation

import com.ai_services.user_management.application.service.UpdateUserRequest
import com.ai_services.user_management.application.service.UserService
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.http.codec.multipart.FilePart
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/api/v1/users")
class UserController(private val userService: UserService) {
    private val log = LoggerFactory.getLogger(UserController::class.java)
    @GetMapping("/me")
    fun me(@AuthenticationPrincipal principal: String) = userService.getByUserId(principal)

    @PatchMapping("/me", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun update(@AuthenticationPrincipal principal: String, @RequestBody request: UpdateUserRequest) =
        userService.updateUser(principal, request)

    @PostMapping("/me/photo", consumes = [MediaType.MULTIPART_FORM_DATA_VALUE])
    fun uploadPhoto(@AuthenticationPrincipal principal: String, @RequestPart("file") file: FilePart): Mono<Map<String, String>> =
        userService.uploadUserPhoto(principal, file)
            .doOnSubscribe { log.info("Upload photo attempt user={}", principal) }
            .map { mapOf("url" to it) }

    @DeleteMapping("/me")
    fun deleteMe(@AuthenticationPrincipal principal: String): Mono<Void> =
        userService.deleteAccount(principal)
}


