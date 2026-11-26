/*
 * Croix Player (2026) | GPL-3.0 License
 */

package com.croix.prime.engine

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.floatOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import okhttp3.OkHttpClient
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

data class DeezerArtistInfo(
    val id: Long,
    val name: String,
    val fans: Int = 0,
    val pictureUrl: String = "",
)

data class DeezerTrackSeed(
    val id: Long,
    val title: String,
    val artist: String,
    val artistId: Long = 0L,
    val album: String = "",
    val rank: Int = 0,
    val durationSeconds: Int = 0,
    val thumbnailUrl: String = "",
)

data class DeezerAudioFeatures(
    val bpm: Float = 0f,
    val gainDb: Float = 0f,
    val releaseYear: Int = 0,
    val rank: Int = 0,
    val artistId: Long = 0L,
)

data class DeezerArtistGraph(
    val artist: DeezerArtistInfo,
    val relatedArtists: List<DeezerArtistInfo>,
    val radioTracks: List<DeezerTrackSeed>,
    val topTracks: List<DeezerTrackSeed>,
)

/**
 * Free, zero-auth Deezer Collaborative-Filtering & Audio-Metadata Graph Client.
 * Powers Spotify/Deezer-grade artist neighborhoods, Flow radio seeds, and BPM/era audio vectors
 * for EVERY listener (with or without a Spotify login!).
 */
class DeezerGraphClient {
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
    }

    private val okHttp = OkHttpClient.Builder()
        .connectTimeout(6, TimeUnit.SECONDS)
        .readTimeout(8, TimeUnit.SECONDS)
        .build()

    private val http = HttpClient(OkHttp) {
        engine { preconfigured = okHttp }
        expectSuccess = false
    }

    private val artistCache = ConcurrentHashMap<String, DeezerArtistInfo>()
    private val artistGraphCache = ConcurrentHashMap<String, DeezerArtistGraph>()
    private val trackFeatureCache = ConcurrentHashMap<String, DeezerAudioFeatures>()

    suspend fun findArtist(rawArtistName: String): DeezerArtistInfo? = withContext(Dispatchers.IO) {
        val clean = cleanPrimaryArtist(rawArtistName)
        if (clean.isBlank()) return@withContext null
        val key = clean.lowercase()
        artistCache[key]?.let { return@withContext it }

        runCatching {
            val encoded = URLEncoder.encode(clean, "UTF-8")
            val body = getJson("https://api.deezer.com/search/artist?q=$encoded&limit=4") ?: return@runCatching null
            val list = body["data"]?.jsonArray.orEmpty().mapNotNull { el ->
                val obj = el as? JsonObject ?: return@mapNotNull null
                val id = obj["id"]?.jsonPrimitive?.longOrNull ?: return@mapNotNull null
                val name = obj["name"]?.jsonPrimitive?.contentOrNull?.trim().orEmpty()
                if (name.isBlank()) return@mapNotNull null
                val fans = obj["nb_fan"]?.jsonPrimitive?.intOrNull ?: 0
                val pic = obj["picture_medium"]?.jsonPrimitive?.contentOrNull
                    ?: obj["picture"]?.jsonPrimitive?.contentOrNull.orEmpty()
                DeezerArtistInfo(id = id, name = name, fans = fans, pictureUrl = pic)
            }
            if (list.isEmpty()) return@runCatching null
            val exact = list.firstOrNull { it.name.equals(clean, ignoreCase = true) }
                ?: list.firstOrNull { it.name.lowercase().contains(key) || key.contains(it.name.lowercase()) }
                ?: list.maxByOrNull { it.fans }
            exact?.also { artistCache[key] = it }
        }.getOrNull()
    }

    /**
     * Fetches Deezer's real Collaborative-Filtering Artist Graph in parallel:
     * - 20 related artists (`/artist/{id}/related`)
     * - 40 Flow radio tracks (`/artist/{id}/radio`)
     * - 12 top tracks (`/artist/{id}/top`)
     */
    suspend fun fetchArtistGraph(rawArtistName: String): DeezerArtistGraph? = withContext(Dispatchers.IO) {
        val clean = cleanPrimaryArtist(rawArtistName)
        if (clean.isBlank()) return@withContext null
        val key = clean.lowercase()
        artistGraphCache[key]?.let { return@withContext it }

        val artist = findArtist(clean) ?: return@withContext null
        runCatching {
            coroutineScope {
                val relatedDef = async { fetchRelatedArtistsById(artist.id, limit = 20) }
                val radioDef = async { fetchArtistRadioById(artist.id, limit = 40) }
                val topDef = async { fetchArtistTopById(artist.id, limit = 12) }
                DeezerArtistGraph(
                    artist = artist,
                    relatedArtists = relatedDef.await(),
                    radioTracks = radioDef.await(),
                    topTracks = topDef.await(),
                )
            }
        }.getOrNull()?.also { graph ->
            if (graph.relatedArtists.isNotEmpty() || graph.radioTracks.isNotEmpty()) {
                artistGraphCache[key] = graph
            }
        }
    }

    suspend fun fetchRelatedArtists(rawArtistName: String, limit: Int = 18): List<DeezerArtistInfo> = withContext(Dispatchers.IO) {
        val clean = cleanPrimaryArtist(rawArtistName)
        if (clean.isBlank()) return@withContext emptyList()
        artistGraphCache[clean.lowercase()]?.let { return@withContext it.relatedArtists.take(limit) }
        val artist = findArtist(clean) ?: return@withContext emptyList()
        fetchRelatedArtistsById(artist.id, limit)
    }

    suspend fun fetchArtistRadio(rawArtistName: String, limit: Int = 35): List<DeezerTrackSeed> = withContext(Dispatchers.IO) {
        val clean = cleanPrimaryArtist(rawArtistName)
        if (clean.isBlank()) return@withContext emptyList()
        artistGraphCache[clean.lowercase()]?.let { return@withContext it.radioTracks.take(limit) }
        val artist = findArtist(clean) ?: return@withContext emptyList()
        fetchArtistRadioById(artist.id, limit)
    }

    /**
     * Fetches real track audio metadata (BPM, gain/loudness, release year, popularity rank)
     * so we can score candidate similarity on tempo & era just like Spotify audio vectors!
     */
    suspend fun fetchTrackAudioFeatures(title: String, artist: String): DeezerAudioFeatures? = withContext(Dispatchers.IO) {
        val cleanArtist = cleanPrimaryArtist(artist)
        val cleanTitle = HybridAlgorithmEngine.normalizeTitleKey(title)
        if (cleanTitle.isBlank()) return@withContext null
        val cacheKey = "$cleanTitle|${cleanArtist.lowercase()}"
        trackFeatureCache[cacheKey]?.let { return@withContext it }

        runCatching {
            val q = if (cleanArtist.isNotBlank()) "artist:\"$cleanArtist\" track:\"$cleanTitle\"" else cleanTitle
            val encoded = URLEncoder.encode(q, "UTF-8")
            val searchRoot = getJson("https://api.deezer.com/search/track?q=$encoded&limit=3")
            val firstHit = searchRoot?.get("data")?.jsonArray?.firstOrNull() as? JsonObject
                ?: run {
                    val loose = URLEncoder.encode("$cleanTitle $cleanArtist".trim(), "UTF-8")
                    getJson("https://api.deezer.com/search/track?q=$loose&limit=3")
                        ?.get("data")?.jsonArray?.firstOrNull() as? JsonObject
                }
                ?: return@runCatching null

            val trackId = firstHit["id"]?.jsonPrimitive?.longOrNull ?: return@runCatching null
            val detail = getJson("https://api.deezer.com/track/$trackId") ?: return@runCatching null
            val bpm = detail["bpm"]?.jsonPrimitive?.floatOrNull ?: 0f
            val gain = detail["gain"]?.jsonPrimitive?.floatOrNull ?: 0f
            val releaseDate = detail["release_date"]?.jsonPrimitive?.contentOrNull.orEmpty()
            val year = releaseDate.take(4).toIntOrNull() ?: 0
            val rank = detail["rank"]?.jsonPrimitive?.intOrNull ?: 0
            val artistId = detail["artist"]?.jsonObject?.get("id")?.jsonPrimitive?.longOrNull ?: 0L
            DeezerAudioFeatures(
                bpm = bpm,
                gainDb = gain,
                releaseYear = year,
                rank = rank,
                artistId = artistId,
            ).also { trackFeatureCache[cacheKey] = it }
        }.getOrNull()
    }

    suspend fun fetchMultiArtistRadio(artistNames: List<String>, perArtistLimit: Int = 20): List<DeezerTrackSeed> =
        withContext(Dispatchers.IO) {
            coroutineScope {
                artistNames.distinctBy { it.lowercase() }.take(5).map { name ->
                    async { fetchArtistRadio(name, limit = perArtistLimit) }
                }.awaitAll().flatten()
            }.distinctBy { "${it.title.lowercase()}|${it.artist.lowercase()}" }
        }

    private suspend fun fetchRelatedArtistsById(artistId: Long, limit: Int): List<DeezerArtistInfo> = runCatching {
        val root = getJson("https://api.deezer.com/artist/$artistId/related?limit=$limit") ?: return@runCatching emptyList()
        root["data"]?.jsonArray.orEmpty().mapNotNull { el ->
            val obj = el as? JsonObject ?: return@mapNotNull null
            val id = obj["id"]?.jsonPrimitive?.longOrNull ?: return@mapNotNull null
            val name = obj["name"]?.jsonPrimitive?.contentOrNull?.trim().orEmpty()
            if (name.isBlank()) return@mapNotNull null
            val fans = obj["nb_fan"]?.jsonPrimitive?.intOrNull ?: 0
            val pic = obj["picture_medium"]?.jsonPrimitive?.contentOrNull
                ?: obj["picture"]?.jsonPrimitive?.contentOrNull.orEmpty()
            DeezerArtistInfo(id = id, name = name, fans = fans, pictureUrl = pic)
        }
    }.getOrElse { emptyList() }

    private suspend fun fetchArtistRadioById(artistId: Long, limit: Int): List<DeezerTrackSeed> = runCatching {
        val root = getJson("https://api.deezer.com/artist/$artistId/radio?limit=$limit") ?: return@runCatching emptyList()
        parseTrackList(root)
    }.getOrElse { emptyList() }

    private suspend fun fetchArtistTopById(artistId: Long, limit: Int): List<DeezerTrackSeed> = runCatching {
        val root = getJson("https://api.deezer.com/artist/$artistId/top?limit=$limit") ?: return@runCatching emptyList()
        parseTrackList(root)
    }.getOrElse { emptyList() }

    private fun parseTrackList(root: JsonObject): List<DeezerTrackSeed> =
        root["data"]?.jsonArray.orEmpty().mapNotNull { el ->
            val obj = el as? JsonObject ?: return@mapNotNull null
            val id = obj["id"]?.jsonPrimitive?.longOrNull ?: return@mapNotNull null
            val title = obj["title_short"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() }
                ?: obj["title"]?.jsonPrimitive?.contentOrNull?.trim().orEmpty()
            val artistObj = obj["artist"]?.jsonObject
            val artist = artistObj?.get("name")?.jsonPrimitive?.contentOrNull?.trim().orEmpty()
            if (title.isBlank() || artist.isBlank()) return@mapNotNull null
            val artistId = artistObj?.get("id")?.jsonPrimitive?.longOrNull ?: 0L
            val albumObj = obj["album"]?.jsonObject
            val album = albumObj?.get("title")?.jsonPrimitive?.contentOrNull.orEmpty()
            val thumb = albumObj?.get("cover_big")?.jsonPrimitive?.contentOrNull
                ?: albumObj?.get("cover_medium")?.jsonPrimitive?.contentOrNull.orEmpty()
            val rank = obj["rank"]?.jsonPrimitive?.intOrNull ?: 0
            val duration = obj["duration"]?.jsonPrimitive?.intOrNull ?: 0
            DeezerTrackSeed(
                id = id,
                title = title,
                artist = artist,
                artistId = artistId,
                album = album,
                rank = rank,
                durationSeconds = duration,
                thumbnailUrl = thumb,
            )
        }

    private suspend fun getJson(url: String): JsonObject? = runCatching {
        val resp = http.get(url) {
            header("Accept", "application/json")
            header("User-Agent", "CroixPlayer/0.5")
        }
        if (resp.status.value !in 200..299) return@runCatching null
        json.parseToJsonElement(resp.bodyAsText()).jsonObject
    }.getOrNull()

    private fun cleanPrimaryArtist(raw: String): String =
        raw.split(",", "&", "feat.", "ft.", " x ", " - ", ignoreCase = true)
            .firstOrNull()?.trim()
            ?.removeSuffix(" - Topic")
            ?.trim()
            .orEmpty()
            .takeUnless {
                it.equals("Unknown Artist", true) ||
                    it.equals("YouTube Music", true) ||
                    it.equals("Various Artists", true)
            }.orEmpty()
}
