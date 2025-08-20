package com.ai_films.movielibrary.application.service

import com.ai_films.movielibrary.domain.model.*
import com.ai_films.movielibrary.domain.queries.MovieSearchFilters
import com.ai_films.movielibrary.infrastructure.repository.MovieRepository
import com.ai_films.movielibrary.infrastructure.repository.SearchRepository
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class MovieService(
    private val repo: MovieRepository,
    private val searchRepo: SearchRepository,
) {
    fun searchByTextGrouped(query: String, filters: MovieSearchFilters, page: Int, size: Int): Mono<GroupedSearchResponse> {
        return if (filters.type.isNullOrBlank()) {
            val moviesFilters = filters.copy(type = "movie")
            val seriesFilters = filters.copy(type = "tv-series")
            val moviesMono = searchRepo.searchText(query, moviesFilters, page, size).map { (items, total) ->
                PagedResponse(items = items, page = page, size = size, total = total)
            }
            val seriesMono = searchRepo.searchText(query, seriesFilters, page, size).map { (items, total) ->
                PagedResponse(items = items, page = page, size = size, total = total)
            }
            Mono.zip(moviesMono, seriesMono).map { GroupedSearchResponse(it.t1, it.t2) }
        } else {
            val type = filters.type
            val primaryMono = searchRepo.searchText(query, filters, page, size).map { (items, total) ->
                PagedResponse(items = items, page = page, size = size, total = total)
            }
            when (type) {
                "movie" -> primaryMono.map { GroupedSearchResponse(it, PagedResponse(emptyList(), page, size, 0)) }
                else -> primaryMono.map { GroupedSearchResponse(PagedResponse(emptyList(), page, size, 0), it) }
            }
        }
    }

    fun searchByGenresGrouped(genres: List<String>, filters: MovieSearchFilters, page: Int, size: Int): Mono<GroupedSearchResponse> {
        return if (filters.type.isNullOrBlank()) {
            val moviesFilters = filters.copy(type = "movie")
            val seriesFilters = filters.copy(type = "tv-series")
            val moviesMono = searchRepo.searchByGenres(genres, moviesFilters, page, size).map { (items, total) ->
                PagedResponse(items = items, page = page, size = size, total = total)
            }
            val seriesMono = searchRepo.searchByGenres(genres, seriesFilters, page, size).map { (items, total) ->
                PagedResponse(items = items, page = page, size = size, total = total)
            }
            Mono.zip(moviesMono, seriesMono).map { GroupedSearchResponse(it.t1, it.t2) }
        } else {
            val type = filters.type
            val primaryMono = searchRepo.searchByGenres(genres, filters, page, size).map { (items, total) ->
                PagedResponse(items = items, page = page, size = size, total = total)
            }
            when (type) {
                "movie" -> primaryMono.map { GroupedSearchResponse(it, PagedResponse(emptyList(), page, size, 0)) }
                else -> primaryMono.map { GroupedSearchResponse(PagedResponse(emptyList(), page, size, 0), it) }
            }
        }
    }

    fun topNewReleasesGrouped(limit: Int): Mono<GroupedListResponse> {
        val flux = searchRepo.topNew(limit)
        val moviesFlux = flux.filter { it.type == "movie" }.collectList()
        val seriesFlux = flux.filter { it.type != "movie" }.collectList()
        return Mono.zip(moviesFlux, seriesFlux).map { GroupedListResponse(it.t1, it.t2) }
    }

    fun details(id: Long): Mono<MovieDetails> = repo.details(id)

    // Запись в историю просмотров в user-management через gateway (внутри сети лучше напрямую)
    private val historyClient: WebClient = WebClient.builder()
        .baseUrl(System.getenv("USER_MANAGEMENT_INTERNAL_URL") ?: "http://user_management_service:8082")
        .build()

    fun recordView(userId: String, movieId: Long, authHeader: String?) {
        val payload = mapOf(
            "movie_id" to movieId.toString(),
            "start_time" to java.time.Instant.now().toString(),
            "duration_watched" to 0,
            "completion_percentage" to 0
        )
        val req = historyClient.post()
            .uri("/api/v1/history/me")
            .bodyValue(payload)
        val reqWithAuth = if (!authHeader.isNullOrBlank()) req.header("Authorization", authHeader) else req.header("X-User-Id", userId)
        reqWithAuth
            .retrieve()
            .toBodilessEntity()
            .onErrorResume { Mono.empty() }
            .subscribe()
    }
}


