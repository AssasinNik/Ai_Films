package com.ai_films.movielibrary.application.service

import com.ai_films.movielibrary.domain.model.*
import com.ai_films.movielibrary.domain.queries.MovieSearchFilters
import com.ai_films.movielibrary.infrastructure.repository.MovieRepository
import com.ai_films.movielibrary.infrastructure.repository.SearchRepository
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class MovieService(
    private val repo: MovieRepository,
    private val searchRepo: SearchRepository,
) {
    fun searchByTextGrouped(query: String, filters: MovieSearchFilters, page: Int, size: Int): Mono<GroupedSearchResponse> {
        val moviesFilters = filters.copy(type = "movie")
        val seriesFilters = filters.copy(type = "tv-series")
        val moviesMono = searchRepo.searchText(query, moviesFilters, page, size).map { (items, total) ->
            PagedResponse(items = items, page = page, size = size, total = total)
        }
        val seriesMono = searchRepo.searchText(query, seriesFilters, page, size).map { (items, total) ->
            PagedResponse(items = items, page = page, size = size, total = total)
        }
        return Mono.zip(moviesMono, seriesMono).map { GroupedSearchResponse(it.t1, it.t2) }
    }

    fun searchByGenresGrouped(genres: List<String>, filters: MovieSearchFilters, page: Int, size: Int): Mono<GroupedSearchResponse> {
        val moviesFilters = filters.copy(type = "movie")
        val seriesFilters = filters.copy(type = "tv-series")
        val moviesMono = searchRepo.searchByGenres(genres, moviesFilters, page, size).map { (items, total) ->
            PagedResponse(items = items, page = page, size = size, total = total)
        }
        val seriesMono = searchRepo.searchByGenres(genres, seriesFilters, page, size).map { (items, total) ->
            PagedResponse(items = items, page = page, size = size, total = total)
        }
        return Mono.zip(moviesMono, seriesMono).map { GroupedSearchResponse(it.t1, it.t2) }
    }

    fun topNewReleasesGrouped(limit: Int): Mono<GroupedListResponse> {
        val flux = searchRepo.topNew(limit)
        val moviesFlux = flux.filter { it.type == "movie" }.collectList()
        val seriesFlux = flux.filter { it.type != "movie" }.collectList()
        return Mono.zip(moviesFlux, seriesFlux).map { GroupedListResponse(it.t1, it.t2) }
    }

    fun details(id: Long): Mono<MovieDetails> = repo.details(id)
}


