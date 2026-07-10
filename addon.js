// IPTV Stremio Addon Core (with debug logging + series (shows) support for BOTH Xtream & Direct M3U)
// Version 1.4.0: Adds Direct M3U series grouping + per‑episode streams
require('dotenv').config();

const { addonBuilder } = require("stremio-addon-sdk");
const crypto = require("crypto");
const LRUCache = require("./lruCache");
const fetch = require('node-fetch');

let redisClient = null;
if (process.env.REDIS_URL) {
    try {
        const { Redis } = require('ioredis');
        redisClient = new Redis(process.env.REDIS_URL, {
            lazyConnect: true,
            maxRetriesPerRequest: 2
        });
        redisClient.on('error', e => console.error('[REDIS] Error:', e.message));
        redisClient.connect().catch(err => console.error('[REDIS] Connect failed:', err.message));
        console.log('[REDIS] Enabled');
    } catch (e) {
        console.warn('[REDIS] ioredis not installed or failed, falling back to in-memory LRU');
        redisClient = null;
    }
}

const ADDON_NAME = "M3U/EPG TV Addon";
const ADDON_ID = "org.stremio.m3u-epg-addon";

const DEBUG_ENV = (process.env.DEBUG_MODE || '').toLowerCase() === 'true';
function makeLogger(cfgDebug) {
    const enabled = !!cfgDebug || DEBUG_ENV;
    return {
        debug: (...a) => { if (enabled) console.log('[DEBUG]', ...a); },
        info:  (...a) => console.log('[INFO]', ...a),
        warn:  (...a) => console.warn('[WARN]', ...a),
        error: (...a) => console.error('[ERROR]', ...a)
    };
}

const CACHE_ENABLED = (process.env.CACHE_ENABLED || 'true').toLowerCase() !== 'false';
const CACHE_TTL_MS = parseInt(process.env.CACHE_TTL_MS || (6 * 3600 * 1000).toString(), 10);
const MAX_CACHE_ENTRIES = parseInt(process.env.MAX_CACHE_ENTRIES || '300', 10);

const dataCache = new LRUCache({ max: MAX_CACHE_ENTRIES, ttl: CACHE_TTL_MS });
const buildPromiseCache = new Map();

async function redisGetJSON(key) {
    if (!redisClient) return null;
    try {
        const raw = await redisClient.get(key);
        if (!raw) return null;
        return JSON.parse(raw);
    } catch { return null; }
}
async function redisSetJSON(key, value, ttl) {
    if (!redisClient) return;
    try {
        await redisClient.set(key, JSON.stringify(value), 'PX', ttl);
    } catch { /* ignore */ }
}

function stableStringify(obj) {
    return JSON.stringify(obj, Object.keys(obj).sort());
}

function createCacheKey(config) {
    const minimal = {
        provider: config.provider,
        m3uUrl: config.m3uUrl,
        epgUrl: config.epgUrl,
        enableEpg: !!config.enableEpg,
        xtreamUrl: config.xtreamUrl,
        xtreamUsername: config.xtreamUsername,
        xtreamUseM3U: !!config.xtreamUseM3U,
        xtreamOutput: config.xtreamOutput,
        epgOffsetHours: config.epgOffsetHours,
        includeSeries: config.includeSeries !== false // default true
    };
    return crypto.createHash('md5').update(stableStringify(minimal)).digest('hex');
}

function createStableM3UItemId(item) {
    const attributes = item.attributes || {};
    const identityParts = [
        attributes['tvg-id'],
        attributes['tvg-name'],
        item.name,
        attributes['group-title']
    ].map(value => String(value || '').trim().replace(/\s+/g, ' ').toLowerCase());

    // Stream URLs can rotate whenever the playlist refreshes. Using one in the
    // ID makes a catalog item impossible to resolve through /meta or /stream
    // after such a refresh. Prefer stable playlist metadata and only fall back
    // to the URL when the EXTINF entry has no identity fields at all.
    const idSource = identityParts.some(Boolean)
        ? [item.type || '', ...identityParts].join('\x1f')
        : String(item.url || '');

    return `iptv_${crypto.createHash('md5').update(idSource).digest('hex').substring(0, 16)}`;
}

class M3UEPGAddon {
    constructor(config = {}, manifestRef) {
        if (!config.provider) {
            config.provider = config.useXtream ? 'xtream' : 'direct';
        }
        this.providerName = config.provider === 'xtream' ? 'xtream' : 'direct';
        this.config = config;
        this.manifestRef = manifestRef;
        this.cacheKey = createCacheKey(config);
        this.updateInterval = 3600000;
        this.channels = []; // live TV
        this.movies = [];   // VOD movies
        this.series = [];   // Series (shows)
        this.seriesInfoCache = new Map(); // seriesId -> { videos: [...], fetchedAt }
        this.epgData = {};
        this.lastUpdate = 0;
        this.log = makeLogger(config.debug);

        // Direct provider may populate this (seriesId -> episodes array)
        this.directSeriesEpisodeIndex = new Map();

        if (typeof this.config.epgOffsetHours === 'string') {
            const n = parseFloat(this.config.epgOffsetHours);
            if (!isNaN(n)) this.config.epgOffsetHours = n;
        }
        if (typeof this.config.epgOffsetHours !== 'number' || !isFinite(this.config.epgOffsetHours))
            this.config.epgOffsetHours = 0;
        if (Math.abs(this.config.epgOffsetHours) > 48)
            this.config.epgOffsetHours = 0;
        if (typeof this.config.includeSeries === 'undefined')
            this.config.includeSeries = true;

        this.log.debug('Addon instance created', {
            provider: this.providerName,
            cacheKey: this.cacheKey,
            epgOffsetHours: this.config.epgOffsetHours,
            includeSeries: this.config.includeSeries
        });
    }

    async loadFromCache() {
        if (!CACHE_ENABLED) return;
        const cacheKey = 'addon:data:' + this.cacheKey;
        let cached = dataCache.get(cacheKey);
        if (!cached && redisClient) {
            cached = await redisGetJSON(cacheKey);
            if (cached) dataCache.set(cacheKey, cached);
        }
        if (cached) {
            this.channels = cached.channels || [];
            this.movies = cached.movies || [];
            this.series = cached.series || [];
            this.epgData = cached.epgData || {};
            this.lastUpdate = cached.lastUpdate || 0;
            // Direct series episodes index is not persisted; rebuild on next fetch
            this.log.debug('Cache hit for data', {
                channels: this.channels.length,
                movies: this.movies.length,
                series: this.series.length,
                lastUpdate: new Date(this.lastUpdate).toISOString()
            });
        }
    }

    async saveToCache() {
        if (!CACHE_ENABLED) return;
        const cacheKey = 'addon:data:' + this.cacheKey;
        const entry = {
            channels: this.channels,
            movies: this.movies,
            series: this.series,
            epgData: this.epgData,
            lastUpdate: this.lastUpdate
        };
        dataCache.set(cacheKey, entry);
        await redisSetJSON(cacheKey, entry, CACHE_TTL_MS);
        this.log.debug('Saved data to cache');
    }

    buildGenresInManifest() {
        if (!this.manifestRef) return;
        const tvCatalog = this.manifestRef.catalogs.find(c => c.id === 'iptv_channels');
        const movieCatalog = this.manifestRef.catalogs.find(c => c.id === 'iptv_movies');
        const seriesCatalog = this.manifestRef.catalogs.find(c => c.id === 'iptv_series');

        // Helper: set catalog.genres (options are added post-build to avoid SDK linter size limits)
        function setGenresOnCatalog(catalog, groups) {
            if (!catalog) return;
            catalog.genres = groups;
        }

        if (tvCatalog) {
            const groups = [
                ...new Set(
                    this.channels
                        .map(c => c.category || c.attributes?.['group-title'])
                        .filter(Boolean)
                        .map(s => s.trim())
                )
            ].sort((a, b) => a.localeCompare(b));
            if (!groups.includes('All Channels')) groups.unshift('All Channels');
            setGenresOnCatalog(tvCatalog, groups);
        }

        if (movieCatalog) {
            const movieGroups = [
                ...new Set(
                    this.movies
                        .map(c => c.category || c.attributes?.['group-title'])
                        .filter(Boolean)
                        .map(s => s.trim())
                )
            ].sort((a, b) => a.localeCompare(b));
            setGenresOnCatalog(movieCatalog, movieGroups);
        }

        if (seriesCatalog) {
            const seriesGroups = [
                ...new Set(
                    this.series
                        .map(c => c.category || c.attributes?.['group-title'])
                        .filter(Boolean)
                        .map(s => s.trim())
                )
            ].sort((a, b) => a.localeCompare(b));
            setGenresOnCatalog(seriesCatalog, seriesGroups);
        }

        this.log.debug('Catalog genres built', {
            tvGenres: tvCatalog?.genres?.length || 0,
            movieGenres: movieCatalog?.genres?.length || 0,
            seriesGenres: seriesCatalog?.genres?.length || 0
        });
    }

    parseM3U(content) {
        const startTs = Date.now();
        const lines = content.split('\n');
        const items = [];
        let currentItem = null;
        for (const raw of lines) {
            const line = raw.trim();
            if (line.startsWith('#EXTINF:')) {
                const matches = line.match(/#EXTINF:(-?\d+)(?:\s+(.*))?,(.*)/);
                if (matches) {
                    currentItem = {
                        duration: parseInt(matches[1]),
                        attributes: this.parseAttributes(matches[2] || ''),
                        name: (matches[3] || '').trim()
                    };
                }
            } else if (line && !line.startsWith('#') && currentItem) {
                currentItem.url = line;
                currentItem.logo = currentItem.attributes['tvg-logo'];
                currentItem.epg_channel_id = currentItem.attributes['tvg-id'] || currentItem.attributes['tvg-name'];
                currentItem.category = currentItem.attributes['group-title'];

                const group = (currentItem.attributes['group-title'] || '').toLowerCase();
                const lower = currentItem.name.toLowerCase();

                const isMovie =
                    group.includes('movie') ||
                    lower.includes('movie') ||
                    this.isMovieFormat(currentItem.name);

                const isSeries =
                    !isMovie && (
                        group.includes('series') ||
                        group.includes('show') ||
                        /\bS\d{1,2}E\d{1,2}\b/i.test(currentItem.name) ||
                        /\bSeason\s?\d+/i.test(currentItem.name)
                    );

                currentItem.type = isSeries ? 'series' : (isMovie ? 'movie' : 'tv');
                currentItem.id = createStableM3UItemId(currentItem);
                items.push(currentItem);
                currentItem = null;
            }
        }
        const ms = Date.now() - startTs;
        this.log.debug('M3U parsed', { lines: lines.length, items: items.length, ms });
        return items;
    }

    parseAttributes(str) {
        const attrs = {};
        const regex = /(\w+(?:-\w+)*)="([^"]*)"/g;
        let m;
        while ((m = regex.exec(str)) !== null) attrs[m[1]] = m[2];
        return attrs;
    }

    isMovieFormat(name) {
        return [/\(\d{4}\)/, /\d{4}\./, /HD$|FHD$|4K$/i].some(p => p.test(name));
    }

    async parseEPG(content) {
        const start = Date.now();
        try {
            const xml2js = require('xml2js');
            const parser = new xml2js.Parser();
            const result = await parser.parseStringPromise(content);
            const epgData = {};
            if (result.tv && result.tv.programme) {
                for (const prog of result.tv.programme) {
                    const ch = prog.$.channel;
                    if (!epgData[ch]) epgData[ch] = [];
                    epgData[ch].push({
                        start: prog.$.start,
                        stop: prog.$.stop,
                        title: prog.title ? prog.title[0]._ || prog.title[0] : 'Unknown',
                        desc: prog.desc ? prog.desc[0]._ || prog.desc[0] : ''
                    });
                }
            }
            this.log.debug('EPG parsed', {
                channels: Object.keys(epgData).length,
                programmes: Object.values(epgData).reduce((a, b) => a + b.length, 0),
                ms: Date.now() - start
            });
            return epgData;
        } catch (e) {
            this.log.warn('EPG parse failed', e.message);
            return {};
        }
    }

    parseEPGTime(s) {
        if (!s) return new Date();
        const m = s.match(/^(\d{14})(?:\s*([+\-]\d{4}))?/);
        if (m) {
            const base = m[1];
            const tz = m[2] || null;
            const year = parseInt(base.slice(0, 4), 10);
            const month = parseInt(base.slice(4, 6), 10) - 1;
            const day = parseInt(base.slice(6, 8), 10);
            const hour = parseInt(base.slice(8, 10), 10);
            const min = parseInt(base.slice(10, 12), 10);
            const sec = parseInt(base.slice(12, 14), 10);
            let date;
            if (tz) {
                const iso = `${year}-${(month + 1).toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}T${hour.toString().padStart(2, '0')}:${min.toString().padStart(2, '0')}:${sec.toString().padStart(2, '0')}${tz}`;
                const parsed = new Date(iso);
                if (!isNaN(parsed.getTime())) date = parsed;
            }
            if (!date) date = new Date(year, month, day, hour, min, sec);
            if (this.config.epgOffsetHours) {
                date = new Date(date.getTime() + this.config.epgOffsetHours * 3600000);
            }
            return date;
        }
        const d = new Date(s);
        if (this.config.epgOffsetHours && !isNaN(d.getTime()))
            return new Date(d.getTime() + this.config.epgOffsetHours * 3600000);
        return d;
    }

    getCurrentProgram(channelId) {
        if (!channelId || !this.epgData[channelId]) return null;
        const now = new Date();
        for (const p of this.epgData[channelId]) {
            const start = this.parseEPGTime(p.start);
            const stop = this.parseEPGTime(p.stop);
            if (now >= start && now <= stop) {
                return { title: p.title, description: p.desc, start, stop, startTime: start, stopTime: stop };
            }
        }
        return null;
    }

    getUpcomingPrograms(channelId, limit = 5) {
        if (!channelId || !this.epgData[channelId]) return [];
        const now = new Date();
        const upcoming = [];
        for (const p of this.epgData[channelId]) {
            const start = this.parseEPGTime(p.start);
            if (start > now && upcoming.length < limit) {
                upcoming.push({
                    title: p.title,
                    description: p.desc,
                    startTime: start,
                    stopTime: this.parseEPGTime(p.stop)
                });
            }
        }
        return upcoming.sort((a, b) => a.startTime - b.startTime);
    }

    async ensureSeriesInfo(seriesId) {
        if (!seriesId) return null;
        if (this.seriesInfoCache.has(seriesId)) return this.seriesInfoCache.get(seriesId);

        try {
            const providerModule = require(`./src/js/providers/${this.providerName}Provider.js`);
            if (typeof providerModule.fetchSeriesInfo === 'function') {
                const info = await providerModule.fetchSeriesInfo(this, seriesId);
                this.seriesInfoCache.set(seriesId, info);
                return info;
            }
        } catch (e) {
            this.log.warn('Series info fetch failed', seriesId, e.message);
        }
        // Fallback empty structure
        const empty = { videos: [] };
        this.seriesInfoCache.set(seriesId, empty);
        return empty;
    }

    async updateData(force = false) {
        const now = Date.now();
        if (!force && CACHE_ENABLED) {
            if (this.lastUpdate && now - this.lastUpdate < this.updateInterval) {
                this.log.debug('Skip update (global interval)');
                return;
            }
            if ((this.channels.length || this.movies.length || this.series.length) && now - this.lastUpdate < 900000) {
                this.log.debug('Skip update (recent minor interval)');
                return;
            }
        }
        try {
            const start = Date.now();
            const providerModule = require(`./src/js/providers/${this.providerName}Provider.js`);
            await providerModule.fetchData(this);
            this.lastUpdate = Date.now();
            if (CACHE_ENABLED) await this.saveToCache();
            this.buildGenresInManifest();
            this.log.debug('Data update complete', {
                channels: this.channels.length,
                movies: this.movies.length,
                series: this.series.length,
                ms: Date.now() - start
            });
        } catch (e) {
            this.log.error('[UPDATE] Failed:', e.message);
        }
    }

    deriveFallbackLogoUrl(item) {
        const logoAttr = item.attributes?.['tvg-logo'];
        if (logoAttr && logoAttr.trim()) return logoAttr;
        const tvgId = item.attributes?.['tvg-id'] || item.attributes?.['tvg-name'];
        if (!tvgId)
            return `https://via.placeholder.com/300x400/333333/FFFFFF?text=${encodeURIComponent(item.name)}`;
        return `logo/${encodeURIComponent(tvgId)}.png`;
    }

    generateMetaPreview(item) {
        const meta = { id: item.id, type: item.type, name: item.name };
        if (item.type === 'tv') {
            const epgId = item.attributes?.['tvg-id'] || item.attributes?.['tvg-name'];
            const current = this.getCurrentProgram(epgId);
            meta.description = current
                ? `📡 Now: ${current.title}${current.description ? `\n${current.description}` : ''}`
                : '📡 Live Channel';
            meta.poster = this.deriveFallbackLogoUrl(item);
            meta.genres = item.category
                ? [item.category]
                : (item.attributes?.['group-title'] ? [item.attributes['group-title']] : ['Live TV']);
            meta.runtime = 'Live';
        } else if (item.type === 'movie') {
            meta.poster = item.poster ||
                item.attributes?.['tvg-logo'] ||
                `https://via.placeholder.com/300x450/CC6600/FFFFFF?text=${encodeURIComponent(item.name)}`;
            meta.year = item.year;
            if (!meta.year) {
                const m = item.name.match(/\((\d{4})\)/);
                if (m) meta.year = parseInt(m[1]);
            }
            meta.description = item.plot || item.attributes?.['plot'] || `Movie: ${item.name}`;
            meta.genres = item.attributes?.['group-title'] ? [item.attributes['group-title']] : ['Movie'];
        } else if (item.type === 'series') {
            meta.poster = item.poster ||
                item.attributes?.['tvg-logo'] ||
                `https://via.placeholder.com/300x450/3366CC/FFFFFF?text=${encodeURIComponent(item.name)}`;
            meta.description = item.plot || item.attributes?.['plot'] || 'Series / Show';
            meta.genres = item.category
                ? [item.category]
                : (item.attributes?.['group-title'] ? [item.attributes['group-title']] : ['Series']);
        }
        return meta;
    }

    getStream(id) {
        // Episode streams
        if (id.startsWith('iptv_series_ep_')) {
            const epEntry = this.lookupEpisodeById(id);
            if (!epEntry) return null;
            return {
                url: epEntry.url,
                title: `${epEntry.title || 'Episode'}${epEntry.season ? ` S${epEntry.season}E${epEntry.episode}` : ''}`,
                behaviorHints: { notWebReady: true }
            };
        }
        const all = [...this.channels, ...this.movies];
        const item = all.find(i => i.id === id);
        if (!item) return null;
        return {
            url: item.url,
            title: item.type === 'tv' ? `${item.name} - Live` : item.name,
            behaviorHints: { notWebReady: true }
        };
    }

    lookupEpisodeById(epId) {
        // Check cached series info
        for (const [, info] of this.seriesInfoCache.entries()) {
            if (info && Array.isArray(info.videos)) {
                const found = info.videos.find(v => v.id === epId);
                if (found) return found;
            }
        }
        // Direct provider inline index
        for (const arr of this.directSeriesEpisodeIndex.values()) {
            const found = arr.find(v => v.id === epId);
            if (found) return found;
        }
        return null;
    }

    async buildSeriesMeta(seriesItem) {
        const seriesIdRaw = seriesItem.series_id || seriesItem.id.replace(/^iptv_series_/, '');
        const info = await this.ensureSeriesInfo(seriesIdRaw);
        const videos = (info?.videos || []).map(v => {
            // Ensure released is a valid ISO date or omit it
            let released = v.released || undefined;
            if (released && typeof released === 'string') {
                // If it's a unix timestamp string, convert
                if (/^\d{9,13}$/.test(released)) {
                    const ts = released.length <= 10
                        ? parseInt(released, 10) * 1000
                        : parseInt(released, 10);
                    const d = new Date(ts);
                    released = !isNaN(d.getTime()) ? d.toISOString() : undefined;
                } else if (!released.includes('T') && !released.includes('Z')) {
                    // Try parsing non-ISO date strings
                    const d = new Date(released);
                    released = !isNaN(d.getTime()) ? d.toISOString() : undefined;
                }
            }
            const video = {
                id: v.id,
                title: v.title,
                season: v.season,
                episode: v.episode,
                thumbnail: v.thumbnail || seriesItem.poster || seriesItem.attributes?.['tvg-logo']
            };
            if (released) video.released = released;
            return video;
        });

        return {
            id: seriesItem.id,
            type: 'series',
            name: seriesItem.name,
            poster: seriesItem.poster ||
                seriesItem.attributes?.['tvg-logo'] ||
                `https://via.placeholder.com/300x450/3366CC/FFFFFF?text=${encodeURIComponent(seriesItem.name)}`,
            description: seriesItem.plot || seriesItem.attributes?.['plot'] || 'Series / Show',
            genres: seriesItem.category
                ? [seriesItem.category]
                : (seriesItem.attributes?.['group-title'] ? [seriesItem.attributes['group-title']] : ['Series']),
            videos
        };
    }

    async getDetailedMetaAsync(id, type) {
        if (type === 'series' || id.startsWith('iptv_series_')) {
            const seriesItem = this.series.find(s => s.id === id);
            if (!seriesItem) return null;
            return await this.buildSeriesMeta(seriesItem);
        }
        // fallback sync path
        return this.getDetailedMeta(id);
    }

    getDetailedMeta(id) {
        const all = [...this.channels, ...this.movies];
        const item = all.find(i => i.id === id);
        if (!item) return null;
        if (item.type === 'tv') {
            const epgId = item.attributes?.['tvg-id'] || item.attributes?.['tvg-name'];
            const current = this.getCurrentProgram(epgId);
            const upcoming = this.getUpcomingPrograms(epgId, 3);
            let description = `📺 CHANNEL: ${item.name}`;
            if (current) {
                const start = current.startTime?.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) || '';
                const end = current.stopTime?.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) || '';
                description += `\n\n📡 NOW: ${current.title}${start && end ? ` (${start}-${end})` : ''}`;
                if (current.description) description += `\n\n${current.description}`;
            }
            if (upcoming.length) {
                description += '\n\n📅 UPCOMING:\n';
                for (const p of upcoming) {
                    description += `${p.startTime.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} - ${p.title}\n`;
                }
            }
            return {
                id: item.id,
                type: 'tv',
                name: item.name,
                poster: this.deriveFallbackLogoUrl(item),
                description,
                genres: item.category
                    ? [item.category]
                    : (item.attributes?.['group-title'] ? [item.attributes['group-title']] : ['Live TV']),
                runtime: 'Live'
            };
        } else {
            let year = item.year;
            if (!year) {
                const m = item.name.match(/\((\d{4})\)/);
                if (m) year = parseInt(m[1]);
            }
            const description = item.plot || item.attributes?.['plot'] || `Movie: ${item.name}`;
            return {
                id: item.id,
                type: 'movie',
                name: item.name,
                poster: item.poster || item.attributes?.['tvg-logo'] ||
                    `https://via.placeholder.com/300x450/CC6600/FFFFFF?text=${encodeURIComponent(item.name)}`,
                description,
                genres: item.attributes?.['group-title'] ? [item.attributes['group-title']] : ['Movie'],
                year
            };
        }
    }
}

async function createAddon(config) {
    const manifest = {
        id: ADDON_ID,
        version: "2.0.0",
        name: ADDON_NAME,
        description: "IPTV addon (M3U / EPG / Xtream) with encrypted configs, caching & series support (Xtream + Direct)",
        resources: ["catalog", "stream", "meta"],
        types: ["tv", "movie", "series"],
        catalogs: [
            {
                type: 'tv',
                id: 'iptv_channels',
                name: 'IPTV Channels',
                extra: [{ name: 'genre' }, { name: 'search' }, { name: 'skip' }],
                genres: []
            },
            {
                type: 'movie',
                id: 'iptv_movies',
                name: 'IPTV Movies',
                extra: [{ name: 'genre' }, { name: 'search' }, { name: 'skip' }],
                genres: []
            },
            {
                type: 'series',
                id: 'iptv_series',
                name: 'IPTV Series',
                extra: [{ name: 'genre' }, { name: 'search' }, { name: 'skip' }],
                genres: []
            }
        ],
        idPrefixes: ["iptv_"],
        behaviorHints: {
            configurable: true,
            configurationRequired: false
        }
    };

    config.instanceId = config.instanceId ||
        (crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(8).toString('hex'));

    const cacheKey = createCacheKey(config);
    const debugFlag = !!config.debug || DEBUG_ENV;
    if (debugFlag) {
        console.log('[DEBUG] createAddon start', { cacheKey, provider: config.provider, includeSeries: config.includeSeries !== false });
    } else {
        console.log(`[ADDON] Cache ${CACHE_ENABLED ? 'ENABLED' : 'DISABLED'} for config ${cacheKey}`);
    }

    if (CACHE_ENABLED && buildPromiseCache.has(cacheKey)) {
        if (debugFlag) console.log('[DEBUG] Reusing build promise', cacheKey);
        return buildPromiseCache.get(cacheKey);
    }

    const buildPromise = (async () => {
        const addonInstance = new M3UEPGAddon(config, manifest);
        await addonInstance.loadFromCache();
        try {
            // Force update on first load to populate genres
            if (!addonInstance.lastUpdate || (Date.now() - addonInstance.lastUpdate > addonInstance.updateInterval)) {
                await addonInstance.updateData(true);
            }
        } catch (e) {
            console.error('[ADDON] Initial update failed:', e);
        }
        addonInstance.buildGenresInManifest();
        
        // Pass the fully populated manifest to builder
        // IMPORTANT: We must ensure 'manifest' object has 'catalogs[].genres' populated BEFORE creating builder
        const builder = new addonBuilder(manifest); 

        builder.defineCatalogHandler(async (args) => {
            const start = Date.now();
            try {
                // Background update
                addonInstance.updateData().catch(() => { });

                let items = [];
                if (args.type === 'tv' && args.id === 'iptv_channels') {
                    items = addonInstance.channels;
                } else if (args.type === 'movie' && args.id === 'iptv_movies') {
                    items = addonInstance.movies;
                } else if (args.type === 'series' && args.id === 'iptv_series') {
                    if (addonInstance.config.includeSeries !== false)
                        items = addonInstance.series;
                }

                const extra = args.extra || {}; // Stremio passes filter/genre here
                let filtered = items;
                
                // If specific genre selected
                if (extra.genre && extra.genre !== 'All Channels') {
                    filtered = filtered.filter(i => {
                        const cat = i.category || i.attributes?.['group-title'];
                        return cat === extra.genre;
                    });
                }
                
                // If searching
                if (extra.search) {
                    const q = extra.search.toLowerCase();
                    filtered = filtered.filter(i => i.name.toLowerCase().includes(q));
                }
                
                // Pagination / Skip
                const skip = extra.skip ? parseInt(extra.skip) : 0;
                const metas = filtered.slice(skip, skip + 100).map(i => addonInstance.generateMetaPreview(i));
                
                if (addonInstance.config.debug) {
                    console.log('[DEBUG] Catalog handler', {
                        type: args.type,
                        id: args.id,
                        genre: extra.genre,
                        totalItems: items.length,
                        filtered: filtered.length,
                        returned: metas.length,
                        skip,
                        ms: Date.now() - start
                    });
                }
                return { metas };
            } catch (e) {
                console.error('[CATALOG] Error', e);
                return { metas: [] };
            }
        });

        builder.defineStreamHandler(async ({ type, id }) => {
            try {
                if (id.startsWith('iptv_series_ep_')) {
                    const stream = addonInstance.getStream(id);
                    if (!stream) return { streams: [] };
                    if (addonInstance.config.debug) {
                        console.log('[DEBUG] Series Episode Stream request', { id, url: stream.url });
                    }
                    return { streams: [stream] };
                }
                const stream = addonInstance.getStream(id);
                if (!stream) return { streams: [] };
                if (addonInstance.config.debug) {
                    console.log('[DEBUG] Stream request', { id, url: stream.url });
                }
                return { streams: [stream] };
            } catch (e) {
                console.error('[STREAM] Error', e);
                return { streams: [] };
            }
        });

        builder.defineMetaHandler(async ({ type, id }) => {
            try {
                if (addonInstance.config.debug) {
                    console.log('[DEBUG] Meta handler called', { type, id, seriesCount: addonInstance.series.length });
                }
                if (type === 'series' || id.startsWith('iptv_series_')) {
                    const meta = await addonInstance.getDetailedMetaAsync(id, 'series');
                    if (addonInstance.config.debug) {
                        console.log('[DEBUG] Series meta result', { id, found: !!meta, videos: meta?.videos?.length });
                    }
                    if (!meta) {
                        // Fallback: Try to find in series list and return basic meta without episodes
                        const seriesItem = addonInstance.series.find(s => s.id === id);
                        if (seriesItem) {
                            const fallbackMeta = {
                                id: seriesItem.id,
                                type: 'series',
                                name: seriesItem.name,
                                poster: seriesItem.poster || seriesItem.attributes?.['tvg-logo'] ||
                                    `https://via.placeholder.com/300x450/3366CC/FFFFFF?text=${encodeURIComponent(seriesItem.name)}`,
                                description: seriesItem.plot || seriesItem.attributes?.['plot'] || 'Series / Show',
                                genres: seriesItem.category
                                    ? [seriesItem.category]
                                    : (seriesItem.attributes?.['group-title'] ? [seriesItem.attributes['group-title']] : ['Series']),
                                videos: []
                            };
                            if (addonInstance.config.debug) {
                                console.log('[DEBUG] Series meta fallback used', { id, name: seriesItem.name });
                            }
                            return { meta: fallbackMeta };
                        }
                    }
                    return { meta: meta || null };
                }
                const meta = addonInstance.getDetailedMeta(id);
                if (addonInstance.config.debug) {
                    console.log('[DEBUG] Meta request', { id, type, found: !!meta });
                }
                return { meta };
            } catch (e) {
                console.error('[META] Error', e);
                return { meta: null };
            }
        });

        const iface = builder.getInterface();

        // POST-BUILD: Inject 'options' into each catalog's genre extra entry.
        // This MUST happen after getInterface() because the SDK linter would reject
        // the large options arrays during build. The manifest object is shared by
        // reference, so mutating it here affects the iface.manifest served by our
        // custom manifest.json route in server.js.
        for (const catalog of manifest.catalogs) {
            if (Array.isArray(catalog.genres) && catalog.genres.length > 0) {
                if (Array.isArray(catalog.extra)) {
                    const genreExtra = catalog.extra.find(e => e.name === 'genre');
                    if (genreExtra) {
                        genreExtra.options = [...catalog.genres];
                    }
                }
            }
        }

        return iface;
    })();

    if (CACHE_ENABLED) buildPromiseCache.set(cacheKey, buildPromise);
    try {
        const iface = await buildPromise;
        return iface;
    } finally {
        // Keep promise cached
    }
}

module.exports = createAddon;
