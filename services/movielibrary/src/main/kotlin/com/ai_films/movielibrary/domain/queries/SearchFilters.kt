package com.ai_films.movielibrary.domain.queries

data class SortOption(
    val orderBy: String = "rating",
    val direction: String = "desc" // asc|desc
)

data class MovieSearchFilters(
    val yearFrom: Int? = null,
    val yearTo: Int? = null,
    val country: String? = null,
    val minRating: Double? = null,
    val maxRating: Double? = null,
    val ageRatingMax: Int? = null,
    val type: String? = null, // movie or tv-series
    val sort: SortOption = SortOption()
)


