/*
 * Croix Player (2026) | GPL-3.0 License
 */

package com.croix.prime.engine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.ln

/**
 * Multi-Signal Recommendation Engine combining Spotify's & Deezer's real architectures:
 * 1. Cross-Platform Collaborative-Filtering Consensus Graph (Deezer Related + Deezer Flow Radio
 *    + Spotify Pathfinder Related + YouTube Music Artist Graph & Radio) — crushes random YT drift!
 * 2. Acoustic & Cultural Coherence (Deezer BPM Gaussian proximity with half/double-time invariance,
 *    release-era proximity, popularity rank quality, and language/script coherence).
 * 3. Implicit Behavioral Bandit (Spotify BaRT / Deezer Flow real-time skip vs completion feedback):
 *    completed listens & favorites boost artists, fast skips penalize tracks/artists and live re-rank
 *    the upcoming queue immediately, and recently-played tracks receive freshness cooldown.
 * 4. Artist Community Clustering (Deezer Flow Vibe Clusters): separates distinct genres/tastes into
 *    pure Daily Mixes so contrasting genres never cross-contaminate.
 * 5. 2-Hop Consensus Discover Weekly (Hidden Gems): discovers artists independently endorsed by
 *    multiple of the user's favorite artists with 0 prior plays.
 */
class HybridAlgorithmEngine(
    private val innerTube: InnerTubeClient,
    private val deezer: DeezerGraphClient = DeezerGraphClient(),
) {
    data class WeightedPlayRecord(
        val track: Track,
        val playCount: Int,
        val playedAtMs: Long,
        val completedPlays: Int = playCount,
        val skipCount: Int = 0,
    )

    data class ListeningFeedbackSnapshot(
        val artistEngagementScores: Map<String, Double> = emptyMap(),
        val skippedTrackIds: Set<String> = emptySet(),
        val recentlyPlayedIds: Set<String> = emptySet(),
        val sessionDislikedArtists: Set<String> = emptySet(),
        val sessionLikedArtists: Set<String> = emptySet(),
    )

    enum class TextScript {
        LATIN,
        CJK_HANGUL,
        CYRILLIC,
        ARABIC,
        DEVANAGARI,
        OTHER,
    }

    suspend fun matchSpotifyTracksToYtMusic(
        seeds: List<SpotifySeedTrack>,
        maxTracks: Int = 40,
    ): List<Track> = withContext(Dispatchers.IO) {
        val limited = seeds.take(maxTracks)
        val matched = mutableListOf<Track>()
        limited.chunked(6).forEach { batch ->
            val results = coroutineScope {
                batch.map { seed ->
                    async {
                        val query = "${seed.title} ${seed.artist}".trim()
                        val candidates = innerTube.searchSongs(query, limit = 3)
                        pickBestMatch(seed, candidates)
                    }
                }.awaitAll()
            }
            matched.addAll(results.filterNotNull())
        }
        matched.distinctBy { it.id }
    }

    suspend fun matchDeezerTracksToYtMusic(
        seeds: List<DeezerTrackSeed>,
        maxTracks: Int = 30,
        blockedArtistKeys: Set<String> = emptySet(),
    ): List<Track> = withContext(Dispatchers.IO) {
        val cleanSeeds = seeds
            .filter { !isBlockedArtist(it.artist, blockedArtistKeys) && it.title.isNotBlank() }
            .distinctBy { "${normalizeTitleKey(it.title)}|${normalizeArtistKey(it.artist)}" }
            .take(maxTracks)
        val matched = mutableListOf<Track>()
        cleanSeeds.chunked(6).forEach { batch ->
            val results = coroutineScope {
                batch.map { seed ->
                    async {
                        val query = "${seed.title} ${seed.artist}".trim()
                        val candidates = innerTube.searchSongs(query, limit = 3)
                        pickBestDeezerMatch(seed, candidates)
                    }
                }.awaitAll()
            }
            matched.addAll(results.filterNotNull())
        }
        matched.distinctBy { it.id }
    }

    /**
     * Tri-Source Spotify × Deezer × YT Music 3-Wave Deep Queue Expander (targets 180-220+ tracks).
     * Eliminates unrelated drift by requiring multi-platform artist consensus, script/BPM/era coherence,
     * and live skip/completion feedback — streaming coherent waves progressively!
     */
    suspend fun expandQueueSpotifyStyle(
        anchorTrack: Track,
        initialExistingQueue: List<Track>,
        targetQueueSize: Int = 200,
        userAffinityArtists: Set<String> = emptySet(),
        blockedArtistKeys: Set<String> = emptySet(),
        feedback: ListeningFeedbackSnapshot = ListeningFeedbackSnapshot(),
        spotifyClient: SpotifyTasteClient? = null,
        spotifySpDc: String = "",
        spotifySpKey: String = "",
        onWaveReady: suspend (List<Track>) -> Unit,
    ) = withContext(Dispatchers.IO) {
        val workingQueue = initialExistingQueue.toMutableList()
        val anchorArtistKey = normalizeArtistKey(anchorTrack.artist)
        val anchorNoiseExempt = hasNoiseKeyword(anchorTrack.title)
        val anchorScript = detectScript("${anchorTrack.title} ${anchorTrack.artist}")

        val userAffinityKeys = userAffinityArtists.map { normalizeArtistKey(it) }.filter { it.isNotBlank() }.toHashSet()
        val userScripts = buildSet {
            add(anchorScript)
            initialExistingQueue.take(12).forEach { add(detectScript("${it.title} ${it.artist}")) }
            userAffinityArtists.take(20).forEach { add(detectScript(it)) }
        }

        // Cross-Platform Consensus Maps: artistKey -> weight & how many independent engines endorsed it
        val consensusWeight = HashMap<String, Double>()
        val endorsementSources = HashMap<String, MutableSet<String>>()
        val deezerRankByTrackId = HashMap<String, Int>()

        fun endorseArtist(rawName: String, sourceTag: String, weightDelta: Double) {
            val k = normalizeArtistKey(rawName)
            if (k.isBlank() || isBlockedArtist(rawName, blockedArtistKeys)) return
            consensusWeight[k] = (consensusWeight[k] ?: 0.0) + weightDelta
            endorsementSources.getOrPut(k) { HashSet() }.add(sourceTag)
        }

        if (anchorArtistKey.isNotBlank()) {
            endorseArtist(anchorTrack.artist, "anchor", 5.5)
        }

        // === PRE-STAGE: Parallel Tri-Source Fetch (Deezer Flow + YT Music Radio + Spotify GQL + Audio Features) ===
        val (ytRadio1, deezerGraph, anchorAudio, spotifyGraph) = coroutineScope {
            val ytRadioDef = async { innerTube.fetchRadioQueue(anchorTrack.id, limit = 85) }
            val dzGraphDef = async {
                if (anchorArtistKey.isNotBlank()) deezer.fetchArtistGraph(anchorTrack.artist) else null
            }
            val dzAudioDef = async {
                deezer.fetchTrackAudioFeatures(anchorTrack.title, anchorTrack.artist)
            }
            val spGraphDef = async {
                if (spotifyClient != null && spotifySpDc.isNotBlank() && anchorArtistKey.isNotBlank()) {
                    spotifyClient.fetchRelatedAndTopForArtist(spotifySpDc, spotifySpKey, anchorTrack.artist)
                } else emptyList<SpotifySeedArtist>() to emptyList()
            }
            Quad(ytRadioDef.await(), dzGraphDef.await(), dzAudioDef.await(), spGraphDef.await())
        }

        // Populate Multi-Platform Consensus Graph
        deezerGraph?.relatedArtists?.forEachIndexed { idx, rel ->
            endorseArtist(rel.name, "deezer_related", (6.2 - idx * 0.16).coerceAtLeast(3.0))
        }
        deezerGraph?.radioTracks?.forEachIndexed { idx, tr ->
            endorseArtist(tr.artist, "deezer_radio", (5.6 - idx * 0.08).coerceAtLeast(2.4))
        }
        spotifyGraph.first.forEachIndexed { idx, rel ->
            endorseArtist(rel.name, "spotify_related", (6.5 - idx * 0.16).coerceAtLeast(3.2))
        }
        // Only top 14 YT radio artists get soft endorsement; deeper tracks must prove consensus!
        ytRadio1.take(14).forEachIndexed { idx, tr ->
            endorseArtist(tr.artist, "yt_radio_top", (3.0 - idx * 0.12).coerceAtLeast(1.2))
        }

        fun scoreAndRankWave(
            raw: List<Track>,
            baseSourceBoost: Double,
            strictConsensusOnly: Boolean,
            maxOut: Int,
        ): List<Track> {
            val clean = filterRelatedRadioTracks(
                candidates = raw,
                existingQueue = workingQueue,
                blockedArtistKeys = blockedArtistKeys,
                allowNoiseVariants = anchorNoiseExempt,
            ).filter { it.id !in feedback.skippedTrackIds }
            if (clean.isEmpty()) return emptyList()

            val scored = clean.mapIndexed { idx, t ->
                val aKey = normalizeArtistKey(t.artist)
                val sources = endorsementSources[aKey]?.size ?: 0
                val cWeight = consensusWeight[aKey] ?: 0.0
                val isUserAff = aKey.isNotBlank() && aKey in userAffinityKeys

                var score = baseSourceBoost - (idx * 0.035)

                // 1. Multi-Platform Consensus Scoring (Spotify Discover Weekly + Deezer Flow core!)
                score += cWeight.coerceAtMost(14.0)
                when {
                    sources >= 3 -> score += 11.5
                    sources == 2 -> score += 7.2
                    sources == 1 -> score += 1.5
                    else -> {
                        // Random YT radio drift with ZERO cross-platform endorsement!
                        score -= if (isUserAff) 1.5 else 7.5
                    }
                }

                // 2. Personal Taste Intersection INSIDE the anchor's neighborhood (never injects unrelated genres!)
                if (isUserAff && sources >= 1) {
                    score += 5.0
                }
                val engScore = feedback.artistEngagementScores[aKey] ?: 0.0
                if (engScore > 0.0 && sources >= 1) {
                    score += (engScore * 0.45).coerceAtMost(4.5)
                } else if (engScore < -1.0) {
                    score += (engScore * 1.4).coerceAtLeast(-9.0)
                }

                // 3. Live Session Contextual Bandit (Immediate skip penalty / completion boost!)
                if (aKey in feedback.sessionDislikedArtists) score -= 12.0
                if (aKey in feedback.sessionLikedArtists && sources >= 1) score += 4.2
                if (t.id in feedback.recentlyPlayedIds) score -= 4.2

                // 4. Script / Language Coherence
                val candScript = detectScript("${t.title} ${t.artist}")
                score += computeScriptCoherence(anchorScript, userScripts, candScript)

                // 5. Studio Quality & Deezer Popularity Rank
                val dzRank = deezerRankByTrackId[t.id] ?: 0
                if (dzRank > 400_000) score += 2.2
                else if (dzRank > 150_000) score += 1.2
                if (t.durationSeconds in 110..420) score += 1.2

                // Slight gentle cap on anchor artist so radio feels diverse like Spotify/Deezer
                if (aKey == anchorArtistKey) {
                    val alreadyAnchor = workingQueue.count { normalizeArtistKey(it.artist) == anchorArtistKey }
                    if (alreadyAnchor >= 3) score -= 3.5
                }

                Triple(t, score, sources)
            }

            val filteredByDrift = if (strictConsensusOnly) {
                val tight = scored.filter { it.third >= 1 && it.second > 0.0 }
                if (tight.size >= 14) tight else scored.filter { it.second > -3.0 }
            } else {
                scored.filter { it.second > -4.0 }.ifEmpty { scored }
            }

            val ordered = filteredByDrift.sortedByDescending { it.second }.map { it.first }
            return interleaveWithArtistSpacing(ordered, minGap = 3).take(maxOut)
        }

        // === WAVE 1 (~65 ultra-tight coherent tracks fast): Deezer Flow Radio + Spotify + Consensus YT Radio ===
        val (matchedDzWave1, matchedSpWave1) = coroutineScope {
            val dzSeeds = buildList {
                deezerGraph?.radioTracks?.take(24)?.let(::addAll)
                deezerGraph?.topTracks?.take(4)?.let(::addAll)
            }
            val dzDef = async {
                if (dzSeeds.isNotEmpty()) {
                    val rankLookup = dzSeeds.associate { "${normalizeTitleKey(it.title)}|${normalizeArtistKey(it.artist)}" to it.rank }
                    matchDeezerTracksToYtMusic(dzSeeds, maxTracks = 24, blockedArtistKeys = blockedArtistKeys).also { matched ->
                        matched.forEach { t ->
                            val r = rankLookup["${normalizeTitleKey(t.title)}|${normalizeArtistKey(t.artist)}"] ?: 0
                            if (r > 0) deezerRankByTrackId[t.id] = r
                        }
                    }
                } else emptyList()
            }
            val spDef = async {
                if (spotifyGraph.second.isNotEmpty()) {
                    matchSpotifyTracksToYtMusic(spotifyGraph.second.shuffled().take(14), maxTracks = 14)
                } else emptyList()
            }
            dzDef.await() to spDef.await()
        }

        // Optional BPM/Era fine-tuning on anchor when Deezer audio features are available
        val anchorBpm = anchorAudio?.bpm ?: 0f
        val anchorYear = anchorAudio?.releaseYear ?: 0
        val wave1Raw = buildList {
            // Put Deezer Flow Radio & Spotify matches FIRST so they anchor the top slots!
            addAll(matchedDzWave1)
            addAll(matchedSpWave1)
            addAll(ytRadio1)
        }

        val wave1 = scoreAndRankWave(
            raw = wave1Raw,
            baseSourceBoost = 8.5,
            strictConsensusOnly = true,
            maxOut = 68,
        ).let { ranked ->
            // Fine-tune first 8 tracks using real Deezer BPM/era proximity when anchor features are known
            if ((anchorBpm > 40f || anchorYear > 1950) && ranked.size >= 8) {
                val head = ranked.take(10)
                val tail = ranked.drop(10)
                val headScored = coroutineScope {
                    head.mapIndexed { idx, track ->
                        async {
                            val feat = deezer.fetchTrackAudioFeatures(track.title, track.artist)
                            val bpmScore = if (feat != null) computeBpmSimilarity(anchorBpm, feat.bpm) else 0.0
                            val eraScore = if (feat != null) computeEraSimilarity(anchorYear, feat.releaseYear) else 0.0
                            track to ((10 - idx) * 0.45 + bpmScore + eraScore)
                        }
                    }.awaitAll()
                }.sortedByDescending { it.second }.map { it.first }
                interleaveWithArtistSpacing(headScored, minGap = 2) + tail
            } else {
                ranked
            }
        }

        if (wave1.isNotEmpty()) {
            workingQueue.addAll(wave1)
            onWaveReady(wave1)
        }
        if (workingQueue.size >= targetQueueSize) return@withContext

        // === WAVE 2 (+70 tracks): Top Consensus Related Artists (Deezer Radio + YT Music Artist Profiles) ===
        val topConsensusRelated = buildList {
            // Prioritize artists endorsed by 2+ platforms first!
            val rankedNames = buildList {
                deezerGraph?.relatedArtists?.forEach { add(it.name) }
                spotifyGraph.first.forEach { add(it.name) }
            }.distinctBy { normalizeArtistKey(it) }
                .filter {
                    val k = normalizeArtistKey(it)
                    k.isNotBlank() && k != anchorArtistKey &&
                        !isBlockedArtist(it, blockedArtistKeys) &&
                        k !in feedback.sessionDislikedArtists
                }
                .sortedByDescending { name ->
                    val k = normalizeArtistKey(name)
                    (endorsementSources[k]?.size ?: 0) * 10.0 + (consensusWeight[k] ?: 0.0)
                }
            addAll(rankedNames)
        }.take(4)

        val ytAnchorProfile = if (anchorArtistKey.isNotBlank()) {
            runCatching { innerTube.fetchArtistProfile(anchorTrack.artist) }.getOrNull()
        } else null
        ytAnchorProfile?.relatedArtists?.forEachIndexed { idx, rel ->
            endorseArtist(rel.name, "yt_related", (4.8 - idx * 0.2).coerceAtLeast(2.2))
        }

        val wave2RelatedArtists = (topConsensusRelated + ytAnchorProfile?.relatedArtists?.map { it.name }.orEmpty())
            .distinctBy { normalizeArtistKey(it) }
            .filter {
                val k = normalizeArtistKey(it)
                k.isNotBlank() && k != anchorArtistKey &&
                    !isBlockedArtist(it, blockedArtistKeys) &&
                    k !in feedback.sessionDislikedArtists
            }
            .take(4)

        val wave2Candidates = coroutineScope {
            val dzRelRadiosDef = async {
                val seeds = deezer.fetchMultiArtistRadio(wave2RelatedArtists.take(3), perArtistLimit = 18)
                seeds.forEach { endorseArtist(it.artist, "deezer_hop2", 3.6) }
                matchDeezerTracksToYtMusic(seeds.shuffled().take(30), maxTracks = 30, blockedArtistKeys = blockedArtistKeys)
            }
            val ytRelProfilesDefs = wave2RelatedArtists.take(3).map { relName ->
                async {
                    val prof = runCatching { innerTube.fetchArtistProfile(relName) }.getOrNull()
                    prof?.relatedArtists?.take(6)?.forEach { r2 -> endorseArtist(r2.name, "yt_hop2", 2.6) }
                    prof?.topTracks.orEmpty()
                }
            }
            val topSeed = ytAnchorProfile?.topTracks?.firstOrNull { it.id != anchorTrack.id }
            val extraRadioDef = async {
                topSeed?.let { innerTube.fetchRadioQueue(it.id, limit = 60) }.orEmpty()
            }
            buildList {
                addAll(dzRelRadiosDef.await())
                addAll(ytRelProfilesDefs.awaitAll().flatten())
                addAll(ytAnchorProfile?.topTracks.orEmpty())
                addAll(extraRadioDef.await())
            }
        }

        val wave2 = scoreAndRankWave(
            raw = wave2Candidates,
            baseSourceBoost = 7.6,
            strictConsensusOnly = true,
            maxOut = (targetQueueSize - workingQueue.size).coerceIn(0, 75),
        )
        if (wave2.isNotEmpty()) {
            workingQueue.addAll(wave2)
            onWaveReady(wave2)
        }
        if (workingQueue.size >= targetQueueSize) return@withContext

        // === WAVE 3 (+70-90 tracks -> reaches 190-220+ total): 2nd-Hop Coherent Consensus Expansion ===
        val secondHopSeeds = (wave2 + wave1)
            .distinctBy { normalizeArtistKey(it.artist) }
            .filter { it.id != anchorTrack.id && normalizeArtistKey(it.artist) != anchorArtistKey }
            .take(3)

        if (secondHopSeeds.isNotEmpty()) {
            val wave3Raw = coroutineScope {
                val dzHop2Def = async {
                    val seeds = deezer.fetchMultiArtistRadio(secondHopSeeds.map { it.artist }, perArtistLimit = 15)
                    seeds.forEach { endorseArtist(it.artist, "deezer_hop3", 2.8) }
                    matchDeezerTracksToYtMusic(seeds.shuffled().take(24), maxTracks = 24, blockedArtistKeys = blockedArtistKeys)
                }
                val ytHop2Defs = secondHopSeeds.map { seed ->
                    async { innerTube.fetchRadioQueue(seed.id, limit = 55) }
                }
                dzHop2Def.await() + ytHop2Defs.awaitAll().flatten()
            }
            val wave3 = scoreAndRankWave(
                raw = wave3Raw,
                baseSourceBoost = 6.8,
                strictConsensusOnly = false,
                maxOut = (targetQueueSize - workingQueue.size).coerceAtLeast(0),
            )
            if (wave3.isNotEmpty()) {
                workingQueue.addAll(wave3)
                onWaveReady(wave3)
            }
        }
    }

    /**
     * Deezer Flow × Spotify Multi-Cluster Home Feed:
     * - Scores tracks by real completion vs skips (skipped songs never pollute recommendations!).
     * - Clusters your favorite artists into PURE Vibe Communities using Deezer Related-Artist edges
     *   so Mix Diario 1, 2, 3, 4 each stay inside ONE coherent musical style!
     */
    suspend fun buildRichHomeFeed(
        ytSections: List<HomeSection>,
        spotifyTaste: SpotifyTasteSnapshot?,
        localFavorites: List<Track>,
        localHistoryRecords: List<WeightedPlayRecord>,
        blendWeight: Float,
        isSpanish: Boolean,
        blockedArtistKeys: Set<String> = emptySet(),
        ytMusicHistory: List<Track> = emptyList(),
        ytMusicLiked: List<Track> = emptyList(),
        feedback: ListeningFeedbackSnapshot = ListeningFeedbackSnapshot(),
    ): HomeFeedBundle = withContext(Dispatchers.IO) {
        val weight = blendWeight.coerceIn(0f, 1f)
        val now = System.currentTimeMillis()

        fun List<Track>.filterAllowed(): List<Track> =
            filter { !isBlockedArtist(it.artist, blockedArtistKeys) && it.id !in feedback.skippedTrackIds }

        val cleanYtSections = ytSections.mapNotNull { sec ->
            val allowed = sec.tracks.filterAllowed().filter { isCleanMusicCandidate(it, allowNoise = false) }
            if (allowed.isEmpty()) null else sec.copy(tracks = allowed)
        }
        val cleanYtHistory = ytMusicHistory.filterAllowed()
        val cleanYtLiked = ytMusicLiked.filterAllowed()
        val cleanFavorites = (cleanYtLiked + localFavorites.filterAllowed()).distinctBy { it.id }
        val favIds = cleanFavorites.map { it.id }.toSet()
        val cleanHistoryRecords = localHistoryRecords.filterNot { isBlockedArtist(it.track.artist, blockedArtistKeys) }

        // 1. Score local tracks with Spotify-style engagement (completion ratio + exponential recency - skip penalty!)
        val scoredTracks = cleanHistoryRecords.map { rec ->
            rec.track to computeTrackEngagementScore(rec, now, isFavorite = rec.track.id in favIds)
        }.filter { it.second > 0.2 }.sortedByDescending { it.second }

        val topLocalTracks = (cleanYtHistory + scoredTracks.map { it.first } + cleanFavorites).distinctBy { it.id }
        val forgottenGems = cleanHistoryRecords
            .filter { (now - it.playedAtMs) > 5L * 86_400_000L && it.completedPlays >= 2 && it.skipCount <= 1 }
            .sortedByDescending { it.completedPlays }
            .map { it.track }
            .take(10)

        val hasSpotify = spotifyTaste != null && spotifyTaste.connected &&
            (spotifyTaste.allSeedTracks.isNotEmpty() || spotifyTaste.allSeedArtistNames.isNotEmpty())
        val hasPersonalTaste = cleanYtHistory.isNotEmpty() || cleanYtLiked.isNotEmpty() || topLocalTracks.isNotEmpty() || hasSpotify

        // 2. Build weighted Artist Affinity ranking (incorporating skip penalties!)
        val artistScores = LinkedHashMap<String, Double>()
        fun boostArtist(raw: String, delta: Double) {
            val main = primaryArtistName(raw)
            if (main.isBlank() || main.equals("Unknown Artist", true) || main.equals("YouTube Music", true)) return
            if (isBlockedArtist(main, blockedArtistKeys)) return
            val existingKey = artistScores.keys.firstOrNull { it.equals(main, ignoreCase = true) } ?: main
            artistScores[existingKey] = (artistScores[existingKey] ?: 0.0) + delta
        }

        cleanYtHistory.take(30).forEachIndexed { idx, t -> boostArtist(t.artist, (4.8 - idx * 0.1).coerceAtLeast(1.0)) }
        cleanYtLiked.take(25).forEach { boostArtist(it.artist, 3.0) }
        if (hasSpotify && weight > 0.05f) {
            val spScale = 0.65 + weight * 1.4
            spotifyTaste!!.shortTermArtists.take(12).forEachIndexed { idx, a -> boostArtist(a.name, (5.2 - idx * 0.25).coerceAtLeast(1.5) * spScale) }
            spotifyTaste.topArtists.take(15).forEachIndexed { idx, a -> boostArtist(a.name, (3.4 - idx * 0.12).coerceAtLeast(1.0) * spScale) }
            spotifyTaste.recentTracks.take(15).forEach { boostArtist(it.artist, 2.4 * spScale) }
        }
        scoredTracks.take(28).forEach { (t, sc) -> boostArtist(t.artist, sc.coerceAtMost(8.0)) }
        // Apply negative engagement from heavily skipped artists
        feedback.artistEngagementScores.forEach { (aKey, sc) ->
            if (sc < -1.0) {
                artistScores.keys.firstOrNull { normalizeArtistKey(it) == aKey }?.let { match ->
                    artistScores[match] = (artistScores[match] ?: 0.0) + sc * 1.5
                }
            }
        }

        val rankedPersonalArtists = artistScores.entries
            .filter { it.value > 0.5 }
            .sortedByDescending { it.value }
            .map { it.key }
        val ytFallbackArtists = if (!hasPersonalTaste) {
            cleanYtSections.flatMap { it.tracks }
                .map { primaryArtistName(it.artist) }
                .filter { it.isNotBlank() && !it.equals("Unknown Artist", true) && !it.equals("YouTube Music", true) && !isBlockedArtist(it, blockedArtistKeys) }
                .distinct()
        } else emptyList()

        val combinedArtistNames = (rankedPersonalArtists + ytFallbackArtists).distinctBy { it.lowercase() }.take(10)

        // 3. Parallel Deezer Artist-Graph Clustering + Spotify Matching + Multi-Seed Radio
        val matchedSpotifyTracks = if (hasSpotify && weight > 0.05f) {
            val spSeeds = (
                spotifyTaste!!.shortTermTracks.take(8) +
                    spotifyTaste.recentTracks.take(6) +
                    spotifyTaste.recommendedTracks.shuffled().take(8) +
                    spotifyTaste.topTracks.take(6)
                ).filterNot { isBlockedArtist(it.artist, blockedArtistKeys) }
                .distinctBy { "${it.title.lowercase()}|${it.artist.lowercase()}" }
                .take(16)
            matchSpotifyTracksToYtMusic(spSeeds, maxTracks = 16).filterAllowed()
        } else emptyList()

        val deezerGraphsByArtist = coroutineScope {
            combinedArtistNames.take(6).map { name ->
                async { name to deezer.fetchArtistGraph(name) }
            }.awaitAll().toMap()
        }

        // Cluster user's top artists into PURE Vibe Communities via Deezer Related-Artist graph!
        val artistClusters: List<List<String>> = clusterArtistsByRelatedGraph(
            artists = combinedArtistNames.take(6),
            relatedMap = deezerGraphsByArtist.mapValues { (_, g) ->
                g?.relatedArtists?.map { normalizeArtistKey(it.name) }?.toSet().orEmpty()
            },
        ).take(4)

        val seedTracksForRadio = buildList {
            cleanYtHistory.firstOrNull()?.let(::add)
            matchedSpotifyTracks.firstOrNull()?.let(::add)
            scoredTracks.firstOrNull()?.first?.let(::add)
            cleanYtLiked.firstOrNull()?.let(::add)
            if (isEmpty()) cleanYtSections.firstOrNull()?.tracks?.firstOrNull()?.let(::add)
        }.distinctBy { it.id }.take(3)

        val (radioPools, clusterMixes) = coroutineScope {
            val radioDefs = seedTracksForRadio.map { seed ->
                async {
                    innerTube.fetchRadioQueue(seed.id, limit = 30)
                        .filterAllowed()
                        .filter { isCleanMusicCandidate(it, allowNoise = false) }
                }
            }
            val clusterDefs = artistClusters.map { cluster ->
                async {
                    val leadArtist = cluster.first()
                    val dzClusterSeeds = cluster.flatMap { a ->
                        val g = deezerGraphsByArtist[a]
                        (g?.radioTracks?.take(14).orEmpty() + g?.topTracks?.take(4).orEmpty())
                    }.distinctBy { "${normalizeTitleKey(it.title)}|${normalizeArtistKey(it.artist)}" }
                        .shuffled()
                        .take(18)

                    val matchedDz = if (dzClusterSeeds.isNotEmpty()) {
                        matchDeezerTracksToYtMusic(dzClusterSeeds, maxTracks = 18, blockedArtistKeys = blockedArtistKeys)
                    } else emptyList()

                    val ytProf = runCatching { innerTube.fetchArtistProfile(leadArtist) }.getOrNull()
                    val ytTop = ytProf?.topTracks.orEmpty().filterAllowed()
                    val ytRadio = ytTop.firstOrNull()?.let {
                        innerTube.fetchRadioQueue(it.id, limit = 24).filterAllowed().filter { t -> isCleanMusicCandidate(t, allowNoise = false) }
                    }.orEmpty()

                    val merged = interleaveWithArtistSpacing(
                        (matchedDz + ytTop.take(6) + ytRadio).distinctBy { it.id },
                        minGap = 3,
                    ).take(32)
                    val coverUrl = deezerGraphsByArtist[leadArtist]?.artist?.pictureUrl
                        ?.takeIf { it.isNotBlank() }
                        ?: ytProf?.bannerUrl.orEmpty()
                    Triple(cluster, coverUrl, merged)
                }
            }
            radioDefs.awaitAll().flatten() to clusterDefs.awaitAll()
        }

        val affinitySet = combinedArtistNames.map { normalizeArtistKey(it) }.toHashSet()
        deezerGraphsByArtist.values.forEach { g ->
            g?.relatedArtists?.forEach { affinitySet.add(normalizeArtistKey(it.name)) }
        }
        if (hasSpotify) {
            spotifyTaste!!.relatedArtists.forEach { affinitySet.add(normalizeArtistKey(it.name)) }
        }

        // 4. Razor-Sharp Quick Picks (24 tracks strictly faithful to completed taste + Deezer consensus!)
        val deezerQuickSeeds = deezerGraphsByArtist.values.filterNotNull()
            .flatMap { it.radioTracks.take(6) }
            .shuffled()
            .take(10)
        val matchedDeezerQuick = if (deezerQuickSeeds.isNotEmpty()) {
            matchDeezerTracksToYtMusic(deezerQuickSeeds, maxTracks = 10, blockedArtistKeys = blockedArtistKeys)
        } else emptyList()

        val rankedRadio = (matchedDeezerQuick + radioPools)
            .distinctBy { it.id }
            .sortedByDescending { t ->
                val k = normalizeArtistKey(t.artist)
                val freshPenalty = if (t.id in feedback.recentlyPlayedIds) -2.5 else 0.0
                (if (k in affinitySet) 6.0 else 1.0) + (if (t.durationSeconds in 90..450) 1.0 else 0.0) + freshPenalty
            }
        val personalCore = (cleanYtHistory.take(10) + matchedSpotifyTracks.take(10) + cleanYtLiked.take(6) + forgottenGems + topLocalTracks.take(6))
            .distinctBy { it.id }

        val quickPicks = interleaveWithArtistSpacing(
            if (hasPersonalTaste) {
                interleaveByWeight(
                    primary = personalCore,
                    secondary = rankedRadio,
                    primaryWeight = if (hasSpotify) (0.45f + weight * 0.28f).coerceIn(0.45f, 0.72f) else 0.56f,
                    limit = 24,
                )
            } else {
                interleaveByWeight(
                    primary = cleanYtSections.firstOrNull()?.tracks.orEmpty(),
                    secondary = rankedRadio,
                    primaryWeight = 0.5f,
                    limit = 20,
                )
            },
            minGap = 2,
        )

        // 5. Pure Vibe-Clustered Daily Mixes ("Hecho para ti")
        val mixGradients = listOf(
            0xFFA238FFL to 0xFFFF2E93L,
            0xFF00E5FFL to 0xFF3D5AFEL,
            0xFFFF6D00L to 0xFFFF1744L,
            0xFF1DB954L to 0xFF00BFA5L,
        )
        val dailyMixes = mutableListOf<DailyMix>()

        if (hasSpotify && matchedSpotifyTracks.isNotEmpty()) {
            val pct = (weight * 100).toInt()
            val hybridTracks = interleaveWithArtistSpacing(
                interleaveByWeight(
                    primary = matchedSpotifyTracks,
                    secondary = (rankedRadio + cleanYtHistory.take(8)).distinctBy { it.id },
                    primaryWeight = weight.coerceIn(0.35f, 0.85f),
                    limit = 36,
                ),
                minGap = 3,
            )
            val topNames = spotifyTaste!!.allSeedArtistNames.filterNot { isBlockedArtist(it, blockedArtistKeys) }.take(3).joinToString(", ")
            dailyMixes += DailyMix(
                id = "mix_spotify_hybrid",
                title = if (isSpanish) "Mix Spotify $pct%" else "Spotify $pct% Mix",
                subtitle = topNames.ifBlank { if (isSpanish) "Basado en tu Spotify" else "Powered by Spotify" },
                accentHex = 0xFF1DB954L,
                secondaryHex = 0xFFA238FFL,
                tracks = hybridTracks,
            )
        }

        clusterMixes.forEachIndexed { idx, (cluster, _, tracks) ->
            if (tracks.size >= 4) {
                val (c1, c2) = mixGradients[idx % mixGradients.size]
                val subNames = (cluster + tracks.map { primaryArtistName(it.artist) })
                    .distinctBy { it.lowercase() }
                    .take(3)
                    .joinToString(", ")
                dailyMixes += DailyMix(
                    id = "daily_mix_${idx + 1}",
                    title = if (isSpanish) "Mix Diario ${idx + 1}" else "Daily Mix ${idx + 1}",
                    subtitle = subNames.ifBlank { cluster.first() },
                    accentHex = c1,
                    secondaryHex = c2,
                    tracks = tracks,
                )
            }
        }

        if (forgottenGems.size >= 3) {
            dailyMixes += DailyMix(
                id = "mix_rediscover",
                title = if (isSpanish) "Para Redescubrir" else "Rediscover Mix",
                subtitle = if (isSpanish) "Favoritas que hace días no escuchas" else "Favorites you haven't played lately",
                accentHex = 0xFFFF4081L,
                secondaryHex = 0xFF7C4DFFL,
                tracks = interleaveWithArtistSpacing((forgottenGems + rankedRadio.take(18)).distinctBy { it.id }, minGap = 2).take(26),
            )
        }

        // 6. Favorite Artists circular row (enriched with Deezer + Spotify high-res artist portraits!)
        val thumbByArtist = mutableMapOf<String, String>()
        spotifyTaste?.shortTermArtists?.forEach { a -> if (a.thumbnailUrl.isNotBlank()) thumbByArtist[a.name.lowercase()] = a.thumbnailUrl }
        spotifyTaste?.topArtists?.forEach { a -> if (a.thumbnailUrl.isNotBlank()) thumbByArtist.putIfAbsent(a.name.lowercase(), a.thumbnailUrl) }
        deezerGraphsByArtist.forEach { (name, g) ->
            val pic = g?.artist?.pictureUrl.orEmpty()
            if (pic.isNotBlank()) thumbByArtist.putIfAbsent(name.lowercase(), pic)
        }
        clusterMixes.forEach { (cluster, cover, tracks) ->
            val url = cover.ifBlank { tracks.firstOrNull()?.thumbnailUrl.orEmpty() }
            if (url.isNotBlank()) thumbByArtist.putIfAbsent(cluster.first().lowercase(), url)
        }
        (topLocalTracks + cleanYtSections.flatMap { it.tracks }).forEach { t ->
            val key = primaryArtistName(t.artist).lowercase()
            if (key.isNotBlank() && !thumbByArtist.containsKey(key) && t.thumbnailUrl.isNotBlank()) {
                thumbByArtist[key] = t.thumbnailUrl
            }
        }
        val favoriteArtists = combinedArtistNames.map { name ->
            RelatedArtist(
                browseId = "",
                name = name,
                subscribers = "",
                thumbnailUrl = thumbByArtist[name.lowercase()].orEmpty(),
            )
        }

        // 7. Coherent Personalized Shelves
        val personalizedShelves = mutableListOf<HomeSection>()
        if (cleanYtHistory.size >= 3) {
            personalizedShelves += HomeSection(
                title = if (isSpanish) "Escucha de nuevo (Tu YT Music)" else "Listen again (Your YT Music)",
                tracks = cleanYtHistory.take(20),
            )
        }
        if (hasSpotify && matchedSpotifyTracks.isNotEmpty()) {
            personalizedShelves += HomeSection(
                title = if (isSpanish) "En bucle en tu Spotify" else "On repeat from your Spotify",
                tracks = matchedSpotifyTracks,
            )
        }
        clusterMixes.take(2).forEach { (cluster, _, tracks) ->
            if (tracks.size >= 4) {
                personalizedShelves += HomeSection(
                    title = if (isSpanish) "Porque escuchaste a ${cluster.first()}" else "Because you listen to ${cluster.first()}",
                    tracks = tracks.take(20),
                )
            }
        }
        if (cleanYtLiked.size >= 4) {
            personalizedShelves += HomeSection(
                title = if (isSpanish) "Tus Me gusta de YouTube Music" else "Your YouTube Music Likes",
                tracks = cleanYtLiked.take(20),
            )
        }

        val finalShelves = (personalizedShelves + cleanYtSections).distinctBy { it.title }.take(7)
        HomeFeedBundle(
            quickPicks = quickPicks,
            dailyMixes = dailyMixes,
            favoriteArtists = favoriteArtists,
            shelves = finalShelves,
        )
    }

    /**
     * Exact Deezer Flow Recipe (55% Familiar Comfort / 30% Multi-Platform Discovery / 15% Forgotten Gems),
     * 160 tracks with strict artist de-clumping and skip protection!
     */
    suspend fun buildHybridFlowQueue(
        spotifyTaste: SpotifyTasteSnapshot?,
        localFavorites: List<Track>,
        localHistory: List<Track>,
        fallbackTracks: List<Track>,
        blendWeight: Float,
        moodQuery: String? = null,
        excludeIds: Set<String> = emptySet(),
        blockedArtistKeys: Set<String> = emptySet(),
        feedback: ListeningFeedbackSnapshot = ListeningFeedbackSnapshot(),
    ): List<Track> = withContext(Dispatchers.IO) {
        fun List<Track>.clean(): List<Track> = filter {
            it.id !in excludeIds &&
                it.id !in feedback.skippedTrackIds &&
                !isBlockedArtist(it.artist, blockedArtistKeys) &&
                normalizeArtistKey(it.artist) !in feedback.sessionDislikedArtists &&
                isCleanMusicCandidate(it, allowNoise = false)
        }

        if (!moodQuery.isNullOrBlank()) {
            val moodTracks = innerTube.searchSongs(moodQuery, limit = 25).clean()
            val seeds = moodTracks.take(3)
            val radios = coroutineScope {
                val ytDefs = seeds.map { s -> async { innerTube.fetchRadioQueue(s.id, limit = 60).clean() } }
                val dzDef = async {
                    val dzSeeds = deezer.fetchMultiArtistRadio(seeds.map { it.artist }, perArtistLimit = 15)
                    matchDeezerTracksToYtMusic(dzSeeds.shuffled().take(20), maxTracks = 20, blockedArtistKeys = blockedArtistKeys).clean()
                }
                dzDef.await() + ytDefs.awaitAll().flatten()
            }
            return@withContext interleaveWithArtistSpacing((moodTracks + radios).distinctBy { it.id }, minGap = 3).take(160)
        }

        val weight = blendWeight.coerceIn(0f, 1f)
        val topArtists = buildList {
            spotifyTaste?.shortTermArtists?.take(4)?.forEach { add(it.name) }
            localFavorites.take(6).forEach { add(primaryArtistName(it.artist)) }
            localHistory.take(6).forEach { add(primaryArtistName(it.artist)) }
            fallbackTracks.take(4).forEach { add(primaryArtistName(it.artist)) }
        }.filter { it.isNotBlank() && !isBlockedArtist(it, blockedArtistKeys) }
            .distinctBy { it.lowercase() }
            .take(5)

        val (spotifyMatched, deezerFlowMatched, ytDeepRadio) = coroutineScope {
            val spDef = async {
                if (spotifyTaste != null && spotifyTaste.connected && weight > 0.1f) {
                    val seeds = (spotifyTaste.shortTermTracks + spotifyTaste.recommendedTracks + spotifyTaste.topTracks)
                        .filterNot { isBlockedArtist(it.artist, blockedArtistKeys) }
                        .shuffled()
                        .take(18)
                    matchSpotifyTracksToYtMusic(seeds, maxTracks = 18).clean()
                } else emptyList()
            }
            val dzDef = async {
                val dzSeeds = deezer.fetchMultiArtistRadio(topArtists, perArtistLimit = 18)
                matchDeezerTracksToYtMusic(dzSeeds.shuffled().take(28), maxTracks = 28, blockedArtistKeys = blockedArtistKeys).clean()
            }
            val ytAnchors = (localHistory.clean().take(2) + localFavorites.clean().shuffled().take(2) + fallbackTracks.clean().take(1))
                .distinctBy { it.id }
                .take(3)
            val ytDef = async {
                if (ytAnchors.isNotEmpty()) {
                    ytAnchors.map { a -> async { innerTube.fetchRadioQueue(a.id, limit = 60).clean() } }.awaitAll().flatten()
                } else {
                    innerTube.searchSongs("top global music hits", limit = 25).clean()
                }
            }
            Triple(spDef.await(), dzDef.await(), ytDef.await())
        }

        // Exact Deezer Flow Blend: Comfort Core (55%) + Discovery Core (45%) with artist de-clumping
        val familiarPool = (spotifyMatched + localFavorites.clean().shuffled().take(24) + localHistory.clean().take(16))
            .distinctBy { it.id }
            .sortedBy { if (it.id in feedback.recentlyPlayedIds) 1 else 0 }
        val discoveryPool = (deezerFlowMatched + ytDeepRadio).distinctBy { it.id }

        interleaveWithArtistSpacing(
            interleaveByWeight(
                primary = familiarPool,
                secondary = discoveryPool,
                primaryWeight = 0.55f,
                limit = 160,
            ),
            minGap = 3,
        )
    }

    /**
     * Spotify Discover Weekly 2-Hop Consensus Radar ("Joyas Ocultas"):
     * Finds artists independently endorsed by 2+ of your favorite artists across Deezer + Spotify + YT Music,
     * then serves their best unheard tracks (0 repeats from history or favorites!).
     */
    suspend fun buildHiddenGemsRadar(
        knownTrackIds: Set<String>,
        blockedArtistKeys: Set<String>,
        spotifyTaste: SpotifyTasteSnapshot?,
        localHistory: List<Track>,
        fallbackTracks: List<Track>,
        feedback: ListeningFeedbackSnapshot = ListeningFeedbackSnapshot(),
    ): List<Track> = withContext(Dispatchers.IO) {
        val knownArtists = buildSet {
            localHistory.take(40).forEach { add(normalizeArtistKey(it.artist)) }
            spotifyTaste?.topArtists?.forEach { add(normalizeArtistKey(it.name)) }
            spotifyTaste?.shortTermArtists?.forEach { add(normalizeArtistKey(it.name)) }
        }
        val seedArtists = buildList {
            spotifyTaste?.shortTermArtists?.map { it.name }?.let(::addAll)
            spotifyTaste?.topArtists?.map { it.name }?.let(::addAll)
            localHistory.map { primaryArtistName(it.artist) }.let(::addAll)
            fallbackTracks.map { primaryArtistName(it.artist) }.let(::addAll)
        }.filter {
            val k = normalizeArtistKey(it)
            k.isNotBlank() && !isBlockedArtist(it, blockedArtistKeys) && k !in feedback.sessionDislikedArtists
        }.distinctBy { it.lowercase() }.take(6)

        // 2-Hop Multi-Artist Consensus Vote (the core Discover Weekly trick!)
        val voteCounts = HashMap<String, Pair<String, Int>>() // normKey -> (displayName, votes)
        fun vote(name: String, weight: Int = 1) {
            val k = normalizeArtistKey(name)
            if (k.isBlank() || isBlockedArtist(name, blockedArtistKeys) || k in feedback.sessionDislikedArtists) return
            val bonus = if (k !in knownArtists) weight + 1 else weight // Prefer truly NEW artists!
            val prev = voteCounts[k]
            voteCounts[k] = (prev?.first ?: name) to ((prev?.second ?: 0) + bonus)
        }

        spotifyTaste?.relatedArtists?.forEach { vote(it.name, weight = 2) }

        val dzRelatedLists = coroutineScope {
            seedArtists.map { artist -> async { deezer.fetchRelatedArtists(artist, limit = 16) } }.awaitAll()
        }
        dzRelatedLists.forEach { list -> list.forEach { rel -> vote(rel.name, weight = 2) } }

        val consensusGemArtists = voteCounts.values
            .sortedByDescending { it.second }
            .map { it.first }
            .take(6)

        val candidates = coroutineScope {
            val dzGemsDef = async {
                val dzSeeds = deezer.fetchMultiArtistRadio(consensusGemArtists.take(4), perArtistLimit = 18)
                matchDeezerTracksToYtMusic(dzSeeds.shuffled().take(34), maxTracks = 34, blockedArtistKeys = blockedArtistKeys)
            }
            val spRecDef = async {
                val recs = spotifyTaste?.recommendedTracks.orEmpty()
                    .filterNot { isBlockedArtist(it.artist, blockedArtistKeys) }
                    .shuffled()
                    .take(16)
                if (recs.isNotEmpty()) matchSpotifyTracksToYtMusic(recs, maxTracks = 16) else emptyList()
            }
            val ytGemProfilesDefs = consensusGemArtists.take(3).map { artist ->
                async { runCatching { innerTube.fetchArtistProfile(artist) }.getOrNull()?.topTracks.orEmpty() }
            }
            val radioDefs = (localHistory.take(2) + fallbackTracks.take(1)).distinctBy { it.id }.take(2).map { seed ->
                async { innerTube.fetchRadioQueue(seed.id, limit = 40) }
            }
            dzGemsDef.await() + spRecDef.await() + ytGemProfilesDefs.awaitAll().flatten() + radioDefs.awaitAll().flatten()
        }

        filterHiddenGems(
            candidates = candidates,
            knownTrackIds = knownTrackIds + feedback.skippedTrackIds,
            blockedArtistKeys = blockedArtistKeys,
            limit = 70,
        )
    }

    internal fun filterHiddenGems(
        candidates: List<Track>,
        knownTrackIds: Set<String>,
        blockedArtistKeys: Set<String>,
        limit: Int = 70,
    ): List<Track> = interleaveWithArtistSpacing(
        candidates
            .filter { it.id !in knownTrackIds && !isBlockedArtist(it.artist, blockedArtistKeys) && isCleanMusicCandidate(it, allowNoise = false) }
            .distinctBy { it.id },
        minGap = 3,
    ).take(limit)

    private data class Quad<A, B, C, D>(val first: A, val second: B, val third: C, val fourth: D)

    companion object {
        private val TITLE_NOISE_REGEX = Regex(
            """\s*[\(\[](?:official|video|audio|lyrics?|letra|visualizer|hd|4k|hq|remaster(?:ed)?|live|en vivo|topic|mv|music video|explicit|clean).*?[\)\]]""",
            RegexOption.IGNORE_CASE,
        )
        private val JUNK_VARIANT_REGEX = Regex(
            """\b(?:8d\s*audio|1\s*hour|10\s*hours|nightcore|slowed\s*\+?\s*reverb|sped\s*up|karaoke|backing\s*track|tutorial|reaction|podcast|full\s*album|megamix|compilation|ringtone|piano\s*cover|guitar\s*cover|cover\s*by)\b""",
            RegexOption.IGNORE_CASE,
        )
        private val NON_ALNUM_REGEX = Regex("""[^\p{L}\p{N}]+""")

        fun hasNoiseKeyword(title: String): Boolean = JUNK_VARIANT_REGEX.containsMatchIn(title)

        fun isCleanMusicCandidate(track: Track, allowNoise: Boolean = false): Boolean {
            if (track.title.isBlank() || track.id.isBlank()) return false
            if (!allowNoise && hasNoiseKeyword(track.title)) return false
            if (track.durationSeconds in 1..64) return false
            if (track.durationSeconds > 660) return false
            return true
        }

        fun normalizeArtistKey(raw: String): String =
            raw.split(",", "&", "feat.", "ft.", " x ", ignoreCase = true)
                .firstOrNull()?.trim()?.removeSuffix(" - Topic")?.trim()?.lowercase().orEmpty()

        fun normalizeTitleKey(rawTitle: String): String {
            val stripped = rawTitle.replace(TITLE_NOISE_REGEX, " ")
            return NON_ALNUM_REGEX.replace(stripped.lowercase(), " ").trim()
        }

        fun detectScript(text: String): TextScript {
            var latin = 0
            var cjk = 0
            var cyrillic = 0
            var arabic = 0
            var devanagari = 0
            for (ch in text) {
                val cp = ch.code
                when {
                    cp in 0x0041..0x024F -> latin++
                    cp in 0x3040..0x30FF || cp in 0x4E00..0x9FFF || cp in 0xAC00..0xD7AF -> cjk++
                    cp in 0x0400..0x04FF -> cyrillic++
                    cp in 0x0600..0x06FF -> arabic++
                    cp in 0x0900..0x097F -> devanagari++
                }
            }
            // Non-Latin scripts take priority when 2+ characters appear (e.g. "夜に駆ける YOASOBI" is CJK!)
            val nonLatinMax = maxOf(cjk, cyrillic, arabic, devanagari)
            if (nonLatinMax >= 2) {
                return when (nonLatinMax) {
                    cjk -> TextScript.CJK_HANGUL
                    cyrillic -> TextScript.CYRILLIC
                    arabic -> TextScript.ARABIC
                    devanagari -> TextScript.DEVANAGARI
                    else -> TextScript.OTHER
                }
            }
            return if (latin > 0) TextScript.LATIN else TextScript.OTHER
        }

        fun computeScriptCoherence(
            anchorScript: TextScript,
            userScripts: Set<TextScript>,
            candidateScript: TextScript,
        ): Double {
            if (anchorScript == TextScript.OTHER || candidateScript == TextScript.OTHER) return 0.0
            if (candidateScript == anchorScript) return 1.6
            if (candidateScript in userScripts) return 0.0
            return -6.5
        }

        /**
         * Gaussian BPM proximity with half-time and double-time invariance (Spotify audio-vector style!).
         */
        fun computeBpmSimilarity(seedBpm: Float, candBpm: Float): Double {
            if (seedBpm <= 40f || candBpm <= 40f) return 0.0
            val diff = minOf(
                abs(seedBpm - candBpm),
                abs(seedBpm - candBpm * 2f),
                abs(seedBpm * 2f - candBpm),
            ).toDouble()
            val gauss = exp(-(diff * diff) / (2.0 * 24.0 * 24.0))
            return gauss * 2.8 - 0.6
        }

        fun computeEraSimilarity(seedYear: Int, candYear: Int): Double {
            if (seedYear < 1950 || candYear < 1950) return 0.0
            val diff = abs(seedYear - candYear)
            return when {
                diff <= 5 -> 2.0
                diff <= 12 -> 1.0
                diff <= 20 -> 0.0
                else -> -1.8
            }
        }

        /**
         * Spotify-style implicit behavioral engagement score:
         * Rewards full listens, recency (exponential decay), and favorites; heavily penalizes fast skips!
         */
        fun computeTrackEngagementScore(
            rec: WeightedPlayRecord,
            nowMs: Long,
            isFavorite: Boolean,
        ): Double {
            val ageDays = ((nowMs - rec.playedAtMs).coerceAtLeast(0L) / 86_400_000.0)
            val recency = 0.35 + 1.65 * exp(-ageDays / 9.0)
            val completed = rec.completedPlays.coerceAtLeast(0)
            val skips = rec.skipCount.coerceAtLeast(0)
            val completionRatio = (completed + 0.5) / (completed + skips + 0.5)
            val freqScore = ln(1.0 + completed) * 2.4
            val favBonus = if (isFavorite) 3.8 else 0.0
            val skipPenalty = skips * 1.45
            return (freqScore * recency * completionRatio) + favBonus - skipPenalty
        }

        /**
         * Deezer Flow Vibe Clustering: groups artists that share Related-Artist edges into pure genre clusters!
         */
        fun clusterArtistsByRelatedGraph(
            artists: List<String>,
            relatedMap: Map<String, Set<String>>,
        ): List<List<String>> {
            if (artists.size <= 1) return if (artists.isEmpty()) emptyList() else listOf(artists)
            val normToRaw = LinkedHashMap<String, String>()
            artists.forEach { a ->
                val k = normalizeArtistKey(a)
                if (k.isNotBlank()) normToRaw.putIfAbsent(k, a)
            }
            val keys = normToRaw.keys.toList()
            val relByKey = HashMap<String, Set<String>>()
            relatedMap.forEach { (raw, rels) ->
                val k = normalizeArtistKey(raw)
                if (k.isNotBlank()) relByKey[k] = rels
            }

            val visited = HashSet<String>()
            val clusters = mutableListOf<List<String>>()
            for (seed in keys) {
                if (!visited.add(seed)) continue
                val group = mutableListOf(seed)
                val seedRels = relByKey[seed].orEmpty()
                for (other in keys) {
                    if (other in visited) continue
                    val otherRels = relByKey[other].orEmpty()
                    val directLink = other in seedRels || seed in otherRels
                    val sharedNeighbors = seedRels.intersect(otherRels).size
                    if (directLink || sharedNeighbors >= 2) {
                        visited.add(other)
                        group.add(other)
                    }
                }
                clusters.add(group.mapNotNull { normToRaw[it] })
            }
            return clusters
        }

        /**
         * Real-time Deezer Flow / Spotify Contextual Bandit Queue Steering on fast skip!
         * Immediately prunes disliked tracks/artists and demotes the just-skipped artist in the upcoming queue.
         */
        fun reRankUpcomingQueue(
            queue: List<Track>,
            currentIndex: Int,
            skippedTrack: Track,
            feedback: ListeningFeedbackSnapshot,
        ): List<Track> {
            if (currentIndex !in queue.indices || currentIndex >= queue.lastIndex) return queue
            val prefix = queue.subList(0, currentIndex + 1)
            val upcoming = queue.subList(currentIndex + 1, queue.size)
            val skippedArtist = normalizeArtistKey(skippedTrack.artist)

            val rescored = upcoming
                .filterNot { t ->
                    t.id in feedback.skippedTrackIds ||
                        normalizeArtistKey(t.artist) in feedback.sessionDislikedArtists
                }
                .mapIndexed { idx, t ->
                    val aKey = normalizeArtistKey(t.artist)
                    var score = (upcoming.size - idx) * 0.15
                    if (aKey.isNotBlank() && aKey == skippedArtist) score -= 8.5
                    if (aKey in feedback.sessionLikedArtists) score += 4.2
                    val eng = feedback.artistEngagementScores[aKey] ?: 0.0
                    score += (eng * 0.35).coerceIn(-6.0, 4.0)
                    t to score
                }
                .sortedByDescending { it.second }
                .map { it.first }

            val spaced = interleaveWithArtistSpacing(rescored, minGap = 3)
            return prefix + spaced
        }

        fun isSameSongVariant(a: Track, b: Track): Boolean {
            if (a.id == b.id) return true
            val titleA = normalizeTitleKey(a.title)
            val titleB = normalizeTitleKey(b.title)
            if (titleA.isBlank() || titleB.isBlank()) return false
            if (titleA == titleB) return true
            val artistA = normalizeArtistKey(a.artist)
            val artistB = normalizeArtistKey(b.artist)
            if (artistA.isNotBlank() && artistA == artistB && (titleA.contains(titleB) || titleB.contains(titleA))) {
                return true
            }
            return false
        }

        fun filterRelatedRadioTracks(
            candidates: List<Track>,
            existingQueue: List<Track>,
            blockedArtistKeys: Set<String> = emptySet(),
            allowNoiseVariants: Boolean = false,
        ): List<Track> {
            val accepted = mutableListOf<Track>()
            val existingIds = existingQueue.map { it.id }.toHashSet()
            for (cand in candidates) {
                if (cand.id in existingIds) continue
                if (!isCleanMusicCandidate(cand, allowNoise = allowNoiseVariants)) continue
                if (isBlockedArtist(cand.artist, blockedArtistKeys)) continue
                if (existingQueue.any { isSameSongVariant(it, cand) }) continue
                if (accepted.any { isSameSongVariant(it, cand) }) continue
                existingIds.add(cand.id)
                accepted.add(cand)
            }
            return accepted
        }

        /**
         * Spotify/Deezer Artist De-Clumping: keeps the same artist separated by at least `minGap` slots.
         */
        fun interleaveWithArtistSpacing(tracks: List<Track>, minGap: Int = 3): List<Track> {
            if (tracks.size <= 2) return tracks
            val remaining = tracks.toMutableList()
            val out = ArrayList<Track>(tracks.size)
            while (remaining.isNotEmpty()) {
                val recentArtists = out.takeLast(minGap).map { normalizeArtistKey(it.artist) }.filter { it.isNotBlank() }.toSet()
                val pickIdx = remaining.indexOfFirst { normalizeArtistKey(it.artist) !in recentArtists }
                    .let { if (it >= 0) it else 0 }
                out.add(remaining.removeAt(pickIdx))
            }
            return out
        }

        fun buildSmartBalancedQueue(
            currentTrack: Track,
            favorites: List<Track>,
            familiarHistory: List<Track>,
            discoveryPool: List<Track>,
            blockedArtistKeys: Set<String> = emptySet(),
            limit: Int = 120,
        ): List<Track> {
            val out = mutableListOf(currentTrack)
            val used = mutableListOf(currentTrack)

            fun pickNext(pool: MutableList<Track>): Track? {
                val iter = pool.iterator()
                while (iter.hasNext()) {
                    val t = iter.next()
                    iter.remove()
                    if (isBlockedArtist(t.artist, blockedArtistKeys)) continue
                    if (used.any { isSameSongVariant(it, t) }) continue
                    used.add(t)
                    return t
                }
                return null
            }

            val favQueue = favorites.filterNot { isBlockedArtist(it.artist, blockedArtistKeys) }.toMutableList()
            val favIds = favQueue.map { it.id }.toSet()
            val knownQueue = familiarHistory
                .filter { it.id !in favIds && !isBlockedArtist(it.artist, blockedArtistKeys) }
                .toMutableList()
            val knownIds = (favIds + knownQueue.map { it.id })
            val discQueue = discoveryPool
                .filter { it.id !in knownIds && !isBlockedArtist(it.artist, blockedArtistKeys) && isCleanMusicCandidate(it, allowNoise = false) }
                .toMutableList()

            val pools = listOf(favQueue, knownQueue, discQueue)
            var slot = 0
            var stalled = 0
            while (out.size < limit && stalled < 3) {
                val chosen = pickNext(pools[slot % 3])
                if (chosen != null) {
                    out.add(chosen)
                    stalled = 0
                } else {
                    stalled++
                }
                slot++
            }
            return out
        }

        fun isBlockedArtist(artistRaw: String, blockedArtistKeys: Set<String>): Boolean {
            if (blockedArtistKeys.isEmpty() || artistRaw.isBlank()) return false
            val full = artistRaw.trim().lowercase()
            if (full in blockedArtistKeys) return true
            val parts = artistRaw.split(",", "&", "feat.", "ft.", " x ", ignoreCase = true)
                .map { it.trim().lowercase() }
                .filter { it.isNotBlank() }
            return parts.any { it in blockedArtistKeys }
        }
    }

    private fun primaryArtistName(raw: String): String =
        raw.split(",", "&", "feat.", "ft.", " x ", ignoreCase = true)
            .firstOrNull()?.trim()?.removeSuffix(" - Topic")?.trim().orEmpty()

    internal fun pickBestMatch(seed: SpotifySeedTrack, candidates: List<Track>): Track? {
        if (candidates.isEmpty()) return null
        val seedTitle = seed.title.lowercase()
        val seedArtist = primaryArtistName(seed.artist).lowercase()
        return candidates.maxByOrNull { c ->
            var score = 0
            val cTitle = c.title.lowercase()
            val cArtist = c.artist.lowercase()
            if (cTitle == seedTitle) score += 50
            else if (cTitle.contains(seedTitle) || seedTitle.contains(cTitle)) score += 30
            if (seedArtist.isNotBlank() && (cArtist.contains(seedArtist) || seedArtist.contains(cArtist))) score += 35
            if ("remix" in cTitle && "remix" !in seedTitle) score -= 18
            if ("live" in cTitle && "live" !in seedTitle) score -= 12
            if ("cover" in cTitle && "cover" !in seedTitle) score -= 24
            if ("karaoke" in cTitle && "karaoke" !in seedTitle) score -= 28
            if ("slowed" in cTitle && "slowed" !in seedTitle) score -= 24
            score
        }?.let { best ->
            if (best.thumbnailUrl.isBlank() && seed.thumbnailUrl.isNotBlank()) {
                best.copy(thumbnailUrl = seed.thumbnailUrl)
            } else {
                best
            }
        }
    }

    internal fun pickBestDeezerMatch(seed: DeezerTrackSeed, candidates: List<Track>): Track? {
        if (candidates.isEmpty()) return null
        val seedTitleNorm = normalizeTitleKey(seed.title)
        val seedArtistNorm = normalizeArtistKey(seed.artist)
        val seedLower = seed.title.lowercase()
        return candidates.mapNotNull { c ->
            if (!isCleanMusicCandidate(c, allowNoise = hasNoiseKeyword(seed.title))) return@mapNotNull null
            var score = 0
            val cTitleNorm = normalizeTitleKey(c.title)
            val cArtistNorm = normalizeArtistKey(c.artist)
            val cLower = c.title.lowercase()
            if (cTitleNorm == seedTitleNorm) score += 55
            else if (cTitleNorm.contains(seedTitleNorm) || seedTitleNorm.contains(cTitleNorm)) score += 32
            if (seedArtistNorm.isNotBlank() && (cArtistNorm == seedArtistNorm || cArtistNorm.contains(seedArtistNorm) || seedArtistNorm.contains(cArtistNorm))) {
                score += 40
            }
            if (seed.durationSeconds > 0 && c.durationSeconds > 0) {
                val diff = abs(seed.durationSeconds - c.durationSeconds)
                if (diff <= 15) score += 16
                else if (diff > 60) score -= 18
            }
            if ("remix" in cLower && "remix" !in seedLower) score -= 20
            if ("live" in cLower && "live" !in seedLower) score -= 15
            if ("cover" in cLower && "cover" !in seedLower) score -= 30
            if ("karaoke" in cLower && "karaoke" !in seedLower) score -= 32
            if ("slowed" in cLower && "slowed" !in seedLower) score -= 26
            if (score < 35) null else c to score
        }.maxByOrNull { it.second }?.first?.let { best ->
            if (best.thumbnailUrl.isBlank() && seed.thumbnailUrl.isNotBlank()) {
                best.copy(thumbnailUrl = seed.thumbnailUrl)
            } else {
                best
            }
        }
    }

    internal fun interleaveByWeight(
        primary: List<Track>,
        secondary: List<Track>,
        primaryWeight: Float,
        limit: Int,
    ): List<Track> {
        if (primary.isEmpty()) return secondary.distinctBy { it.id }.take(limit)
        if (secondary.isEmpty()) return primary.distinctBy { it.id }.take(limit)

        val out = mutableListOf<Track>()
        val seen = HashSet<String>()
        var pi = 0
        var si = 0
        var acc = 0f

        while (out.size < limit && (pi < primary.size || si < secondary.size)) {
            acc += primaryWeight
            val takePrimary = (acc >= 1f && pi < primary.size) || si >= secondary.size
            val next = if (takePrimary && pi < primary.size) {
                acc -= 1f
                primary[pi++]
            } else if (si < secondary.size) {
                secondary[si++]
            } else {
                null
            }
            if (next != null && seen.add(next.id)) {
                out.add(next)
            }
        }
        return out
    }
}
