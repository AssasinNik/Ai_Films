package com.ai_films.movielibrary.domain.model

data class GroupedSearchResponse(
    val movies: PagedResponse<MediaItemCard>,
    val series: PagedResponse<MediaItemCard>
)

data class GroupedListResponse(
    val movies: List<MediaItemCard>,
    val series: List<MediaItemCard>
)


