package com.ai_services.user_management.infrastructure.config

import io.minio.BucketExistsArgs
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.minio.SetBucketPolicyArgs
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class MinioConfig(
    @Value("\${minio.url}") private val minioUrl: String,
    @Value("\${minio.access-key}") private val accessKey: String,
    @Value("\${minio.secret-key}") private val secretKey: String,
    @Value("\${minio.bucket.user-photos}") private val userPhotosBucket: String,
    @Value("\${minio.public-url}") val publicUrl: String
) {
    @Bean
    fun minioClient(): MinioClient = MinioClient.builder()
        .endpoint(minioUrl)
        .credentials(accessKey, secretKey)
        .build()

    @Bean
    fun ensureBuckets(minioClient: MinioClient): Any {
        val exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(userPhotosBucket).build())
        if (!exists) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(userPhotosBucket).build())
        }
        // Ensure public read policy for user-photos
        val policy = """
            {
              "Version":"2012-10-17",
              "Statement":[
                {"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::${userPhotosBucket}"]},
                {"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::${userPhotosBucket}/*"]}
              ]
            }
        """.trimIndent()
        minioClient.setBucketPolicy(
            SetBucketPolicyArgs.builder()
                .bucket(userPhotosBucket)
                .config(policy)
                .build()
        )
        return Any()
    }
}


