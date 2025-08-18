package com.ai_films.movielibrary.infrastructure.config

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.transport.rest_client.RestClientTransport
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.net.URI

@Configuration
class ElasticsearchConfig(
    @Value("\${elasticsearch.url}") private val esUrl: String,
) {
    @Bean
    fun elasticsearchClient(): ElasticsearchClient {
        val uri = URI.create(esUrl)
        val builder: RestClientBuilder = RestClient.builder(HttpHost(uri.host, uri.port, uri.scheme))
        val restClient = builder.build()
        val transport = RestClientTransport(restClient, JacksonJsonpMapper())
        return ElasticsearchClient(transport)
    }
}


