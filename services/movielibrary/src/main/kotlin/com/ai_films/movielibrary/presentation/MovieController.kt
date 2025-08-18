package com.ai_films.movielibrary.presentation

import com.ai_films.movielibrary.application.service.MovieService
import com.ai_films.movielibrary.domain.model.GroupedListResponse
import com.ai_films.movielibrary.domain.model.GroupedSearchResponse
import com.ai_films.movielibrary.domain.model.MovieDetails
import com.ai_films.movielibrary.domain.queries.MovieSearchFilters
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.Positive
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/movies")
@Validated
class MovieController(private val service: MovieService) {

    @GetMapping("/search/text")
    fun searchByText(
        @RequestParam q: String,
        @RequestParam(required = false) type: String?,
        @RequestParam(required = false) yearFrom: Int?,
        @RequestParam(required = false) yearTo: Int?,
        @RequestParam(required = false) country: String?,
        @RequestParam(required = false) minRating: Double?,
        @RequestParam(required = false) maxRating: Double?,
        @RequestParam(required = false) ageRatingMax: Int?,
        @RequestParam(defaultValue = "rating") orderBy: String,
        @RequestParam(defaultValue = "desc") direction: String,
        @RequestParam(defaultValue = "1") @Positive page: Int,
        @RequestParam(defaultValue = "20") @Min(1) @Max(100) size: Int,
    ): Mono<GroupedSearchResponse> {
        val filters = MovieSearchFilters(
            yearFrom = yearFrom,
            yearTo = yearTo,
            country = country,
            minRating = minRating,
            maxRating = maxRating,
            ageRatingMax = ageRatingMax,
            type = type,
            sort = com.ai_films.movielibrary.domain.queries.SortOption(orderBy, direction)
        )
        return service.searchByTextGrouped(q, filters, page, size)
    }

    @GetMapping("/search/genres")
    fun searchByGenres(
        @RequestParam genres: List<String>,
        @RequestParam(required = false) type: String?,
        @RequestParam(required = false) yearFrom: Int?,
        @RequestParam(required = false) yearTo: Int?,
        @RequestParam(required = false) country: String?,
        @RequestParam(required = false) minRating: Double?,
        @RequestParam(required = false) maxRating: Double?,
        @RequestParam(required = false) ageRatingMax: Int?,
        @RequestParam(defaultValue = "1") @Positive page: Int,
        @RequestParam(defaultValue = "20") @Min(1) @Max(100) size: Int,
    ): Mono<GroupedSearchResponse> {
        val filters = MovieSearchFilters(
            yearFrom = yearFrom,
            yearTo = yearTo,
            country = country,
            minRating = minRating,
            maxRating = maxRating,
            ageRatingMax = ageRatingMax,
            type = type,
        )
        return service.searchByGenresGrouped(genres, filters, page, size)
    }

    @GetMapping("/new")
    fun topNew(@RequestParam(defaultValue = "10") @Min(1) @Max(50) limit: Int): Mono<GroupedListResponse> = service.topNewReleasesGrouped(limit)

    @GetMapping("/{id}")
    fun details(@PathVariable id: Long): Mono<MovieDetails> = service.details(id)
}


