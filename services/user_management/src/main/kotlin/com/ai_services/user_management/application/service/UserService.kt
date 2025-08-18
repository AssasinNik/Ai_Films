package com.ai_services.user_management.application.service

import com.ai_services.user_management.domain.user.User
import com.ai_services.user_management.infrastructure.repository.UserRepository
import com.ai_services.user_management.infrastructure.config.MinioConfig
import io.minio.GetPresignedObjectUrlArgs
import io.minio.MinioClient
import io.minio.PutObjectArgs
import io.minio.http.Method
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.codec.multipart.FilePart
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.nio.file.Files
import java.nio.file.Path
import jakarta.validation.constraints.Size
import java.util.UUID

data class UpdateUserRequest(@field:Size(min = 3, max = 50) val username: String?)

@Service
class UserService(
    private val userRepository: UserRepository,
    private val minioClient: MinioClient,
    @Value("\${minio.bucket.user-photos}") private val userPhotosBucket: String,
    private val minioConfig: MinioConfig
) {
    fun getByUserId(userId: String): Mono<User> = userRepository.findByUserId(userId)

    fun updateUser(userId: String, request: UpdateUserRequest): Mono<User> =
        userRepository.findByUserId(userId)
            .switchIfEmpty(Mono.error(NoSuchElementException("User not found")))
            .flatMap { user ->
                userRepository.save(user.copy(username = request.username ?: user.username))
            }

    fun uploadUserPhoto(userId: String, file: FilePart): Mono<String> {
        val log = LoggerFactory.getLogger(UserService::class.java)
        val original = file.filename().ifBlank { "upload" }
        val safeName = original.replace(Regex("[^A-Za-z0-9._-]"), "_")
        val objectName = "${userId}/${UUID.randomUUID()}_${safeName}"
        val contentType = file.headers().contentType?.toString() ?: "application/octet-stream"
        val temp: Path = Files.createTempFile("upload_", "_img")

        return file.transferTo(temp)
            .then(Mono.fromCallable {
                Files.newInputStream(temp).use { input ->
                    val size = Files.size(temp)
                    minioClient.putObject(
                        PutObjectArgs.builder()
                            .bucket(userPhotosBucket)
                            .`object`(objectName)
                            .stream(input, size, -1)
                            .contentType(contentType)
                            .build()
                    )
                }
                Files.deleteIfExists(temp)
                val public = "${minioConfig.publicUrl.trimEnd('/')}/${userPhotosBucket}/${objectName}"
                log.info("Uploaded photo user={} object={} publicUrl={}", userId, objectName, public)
                public
            })
            .flatMap { url ->
                userRepository.findByUserId(userId).flatMap { user ->
                    userRepository.save(user.copy(photoUrl = url)).thenReturn(url)
                }
            }
    }
}


