package com.ai_films.movielibrary.infrastructure.repository

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.SortOptions
import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery
import co.elastic.clients.elasticsearch._types.query_dsl.MultiMatchQuery
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.search.Hit
import com.ai_films.movielibrary.domain.model.MediaItemCard
import com.ai_films.movielibrary.domain.queries.MovieSearchFilters
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.core.type.TypeReference
import co.elastic.clients.json.JsonData

@Repository
class SearchRepository(
    private val es: ElasticsearchClient,
    private val redis: ReactiveStringRedisTemplate,
) {
    private val indexName = "movies"
    private val objectMapper = jacksonObjectMapper()

    fun searchText(query: String, filters: MovieSearchFilters, page: Int, size: Int): Mono<Pair<List<MediaItemCard>, Long>> {
        val from = (page - 1) * size

        val bool = BoolQuery.Builder()
            .must(Query.of { q -> q.multiMatch { mm ->
                mm.query(query)
                    .fields("title^3")
                    .fields("alternative_name^2")
                    .fields("en_name^1")
                    .fields("description^1")
            } })
            .filter(buildFilterQueries(filters))
            .build()

        val sort = when (filters.sort.orderBy.lowercase()) {
            "year" -> SortOptions.Builder().field { f -> f.field("year").order(toEsOrder(filters.sort.direction)) }.build()
            "title" -> SortOptions.Builder().field { f -> f.field("title.keyword").order(toEsOrder(filters.sort.direction)) }.build()
            else -> SortOptions.Builder().field { f -> f.field("ratings.kp").order(toEsOrder(filters.sort.direction)) }.build()
        }

        val req = SearchRequest.Builder()
            .index(indexName)
            .from(from)
            .size(size)
            .sort(sort)
            .query { q -> q.bool(bool) }
            .build()

        return Mono.fromCallable { es.search(req, Map::class.java) }
            .map { resp ->
                val total = resp.hits().total()?.value() ?: 0L
                val items = resp.hits().hits().mapNotNull { hit: Hit<Map<*, *>> ->
                    toCard(hit.source())
                }
                items to total
            }
    }

    fun searchByGenres(genres: List<String>, filters: MovieSearchFilters, page: Int, size: Int): Mono<Pair<List<MediaItemCard>, Long>> {
        val from = (page - 1) * size

        // Nested жанры: суммарный скор по количеству совпадений жанров
        val bool = BoolQuery.Builder()
            .must(
                Query.of { q ->
                    q.nested { n ->
                        n.path("genres")
                            .scoreMode(co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode.Sum)
                            .query { nq ->
                                nq.bool { b ->
                                    genres.fold(b) { acc, g ->
                                        acc.should { s -> s.term { t -> t.field("genres.name.keyword").value(g) } }
                                    }
                                    b.minimumShouldMatch("1")
                                }
                            }
                    }
                }
            )
            .filter(buildFilterQueries(filters))
            .build()

        val primarySort = when (filters.sort.orderBy.lowercase()) {
            "year" -> SortOptions.Builder().field { f -> f.field("year").order(toEsOrder(filters.sort.direction)) }.build()
            "title" -> SortOptions.Builder().field { f -> f.field("title.keyword").order(toEsOrder(filters.sort.direction)) }.build()
            else -> SortOptions.Builder().field { f -> f.field("ratings.kp").order(toEsOrder(filters.sort.direction)) }.build()
        }

        val req = SearchRequest.Builder()
            .index(indexName)
            .from(from)
            .size(size)
            .sort(primarySort)
            .sort { s -> s.field { f -> f.field("_score").order(SortOrder.Desc) } }
            .query { q -> q.bool(bool) }
            .build()

        return Mono.fromCallable { es.search(req, Map::class.java) }
            .map { resp ->
                val total = resp.hits().total()?.value() ?: 0L
                val items = resp.hits().hits().mapNotNull { hit -> toCard(hit.source()) }
                items to total
            }
    }

    fun topNew(limit: Int): Flux<MediaItemCard> {
        val cacheKey = "movies:new:$limit"
        val readCache = redis.opsForValue().get(cacheKey).flatMapMany { str ->
            if (!str.isNullOrBlank()) {
                val list: List<MediaItemCard> = try {
                    objectMapper.readValue(str, object : TypeReference<List<MediaItemCard>>() {})
                } catch (_: Exception) { emptyList() }
                Flux.fromIterable(list)
            } else Flux.empty()
        }

        val fetchEs = Mono.fromCallable {
            val req = SearchRequest.Builder()
                .index(indexName)
                .size(limit)
                .sort { s -> s.field { f -> f.field("created_at").order(SortOrder.Desc) } }
                .sort { s -> s.field { f -> f.field("ratings.kp").order(SortOrder.Desc) } }
                .build()
            es.search(req, Map::class.java)
        }.map { resp -> resp.hits().hits().mapNotNull { toCard(it.source()) } }
            .flatMapMany { items ->
                val json = try { objectMapper.writeValueAsString(items) } catch (_: Exception) { null }
                if (json != null) {
                    redis.opsForValue().set(cacheKey, json).thenMany(Flux.fromIterable(items))
                } else {
                    Flux.fromIterable(items)
                }
            }

        return readCache.switchIfEmpty(fetchEs)
    }

    private fun toCard(src: Map<*, *>?): MediaItemCard? {
        if (src == null) return null
        val id = (src["movie_id"] as? Number)?.toLong() ?: return null
        val title = (src["title"] as? String) ?: (src["alternative_name"] as? String) ?: src["en_name"] as? String
        val rating = ((src["ratings"] as? Map<*, *>)?.get("kp") as? Number)?.toDouble()
        val year = (src["year"] as? Number)?.toInt()
        val genres = (src["genres"] as? List<Map<*, *>>)?.mapNotNull { it["name"] as? String } ?: emptyList()
        val countries = (src["countries"] as? List<Map<*, *>>)?.mapNotNull { it["name"] as? String } ?: emptyList()
        val description = src["description"] as? String
        val poster = src["poster_url"] as? String
        val type = (src["type"] as? String) ?: "movie"
        return MediaItemCard(id, title, rating, year, genres, description, countries, poster, type)
    }

    private fun buildFilterQueries(filters: MovieSearchFilters): List<Query> {
        val list = mutableListOf<Query>()
        filters.type?.let { t -> list += Query.of { q -> q.term { term -> term.field("type.keyword").value(t) } } }
        filters.yearFrom?.let { y -> list += Query.of { q -> q.range { r -> r.field("year").gte(JsonData.of(y)) } } }
        filters.yearTo?.let { y -> list += Query.of { q -> q.range { r -> r.field("year").lte(JsonData.of(y)) } } }
        filters.country?.let { c ->
            list += Query.of { q ->
                q.nested { n ->
                    n.path("countries")
                        .query { nq -> nq.term { t -> t.field("countries.name.keyword").value(c) } }
                }
            }
        }
        filters.minRating?.let { r -> list += Query.of { q -> q.range { rr -> rr.field("ratings.kp").gte(JsonData.of(r)) } } }
        filters.maxRating?.let { r -> list += Query.of { q -> q.range { rr -> rr.field("ratings.kp").lte(JsonData.of(r)) } } }
        filters.ageRatingMax?.let { a -> list += Query.of { q -> q.range { rr -> rr.field("age_rating").lte(JsonData.of(a)) } } }
        return list
    }

    private fun toEsOrder(dir: String): SortOrder = if (dir.equals("asc", true)) SortOrder.Asc else SortOrder.Desc
}


