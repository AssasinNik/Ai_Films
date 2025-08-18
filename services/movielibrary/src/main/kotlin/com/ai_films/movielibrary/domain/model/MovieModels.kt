package com.ai_films.movielibrary.domain.model

data class MediaItemCard(
    val id: Long,
    val title: String?,
    val rating: Double?,
    val year: Int?,
    val genres: List<String>,
    val description: String?,
    val country: List<String>,
    val posterUrl: String?,
    val type: String, // movie or tv-series
)

data class PagedResponse<T>(
    val items: List<T>,
    val page: Int,
    val size: Int,
    val total: Long
)

data class MovieDetails(
    val id: Long,
    val title: String?,
    val alternativeName: String?,
    val enName: String?,
    val description: String?,
    val ageRating: Int?,
    val year: Int?,
    val type: String,
    val posterUrl: String?,
    val backdropUrl: String?,
    val ratingKp: Double?,
    val ratingImdb: Double?,
    val genres: List<String>,
    val countries: List<String>,
    val people: List<PersonCredit>,
    val seasons: List<SeasonDetails>
)

data class PersonCredit(
    val id: Long?,
    val name: String?,
    val role: String?,
    val characterName: String?
)

data class SeasonDetails(
    val id: Long,
    val seasonNumber: Int,
    val episodesCount: Int?,
    val airDate: String?,
    val posterUrl: String?,
    val description: String?
)


