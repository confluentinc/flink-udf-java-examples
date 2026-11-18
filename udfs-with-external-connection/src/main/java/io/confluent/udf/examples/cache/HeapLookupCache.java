/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.udf.examples.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.time.Duration;
import java.util.Locale;
import java.util.Optional;

/**
 * A bounded, read-through cache on the JVM heap for the results of external calls made from a UDF.
 *
 * <p>Design choices, each of which matters inside the memory-limited UDF runtime:
 *
 * <ul>
 *   <li><b>Bounded by estimated bytes, not entry count.</b> Response sizes vary, so an entry count
 *       is not a memory bound. Entries are weighed by {@link #estimateBytes} and the least valuable
 *       ones are evicted once {@code maxBytes} is reached.
 *   <li><b>Every entry expires.</b> Found values live for {@code ttl}. "Not found" results (the
 *       loader returned {@code null}) are cached for the usually shorter {@code negativeTtl}, so a
 *       bad key doesn't trigger a remote call for every record.
 *   <li><b>Failures are never cached.</b> If the loader throws, the exception reaches the caller
 *       and the next lookup of that key tries again.
 *   <li><b>No lock is held during the remote call.</b> A miss runs the loader on the caller's
 *       thread and stores the result afterwards. If two threads miss the same key at once, both
 *       call the service. That is harmless here, and it means a slow call never blocks lookups of
 *       other keys.
 *   <li><b>No background threads.</b> Eviction and expiry run on the caller's thread, so the cache
 *       adds no thread stacks or thread pools to the UDF process.
 * </ul>
 *
 * <p>The cache belongs to one function instance and lives only as long as that instance. Nothing is
 * shared between parallel instances, and it is not checkpointed. A restart starts with an empty
 * cache, so it reduces calls but correctness must never depend on it.
 */
public final class HeapLookupCache {

    /**
     * Rough per-entry overhead in bytes: the cache node, the two String objects and their array
     * headers, and the Optional wrapper. A deliberate overestimate.
     */
    static final int ENTRY_OVERHEAD_BYTES = 160;

    /** Loads the value for a key on a cache miss. */
    @FunctionalInterface
    public interface Loader {
        /**
         * Returns the value for {@code key}, or {@code null} if there is none (cached for the
         * negative TTL). Throws to signal a failure that should not be cached.
         */
        String load(String key) throws Exception;
    }

    private final Cache<String, Optional<String>> cache;
    private final long maxBytes;

    /**
     * Creates a cache.
     *
     * @param maxBytes upper bound on the estimated heap used by cached entries
     * @param ttl how long a found value stays cached
     * @param negativeTtl how long a "not found" result stays cached
     */
    public HeapLookupCache(long maxBytes, Duration ttl, Duration negativeTtl) {
        this(maxBytes, ttl, negativeTtl, Ticker.systemTicker());
    }

    /** Visible for testing: lets tests control time. */
    HeapLookupCache(long maxBytes, Duration ttl, Duration negativeTtl, Ticker ticker) {
        this.maxBytes = maxBytes;
        final long ttlNanos = ttl.toNanos();
        final long negativeTtlNanos = negativeTtl.toNanos();
        this.cache =
                Caffeine.newBuilder()
                        .maximumWeight(maxBytes)
                        .weigher(HeapLookupCache::estimateBytes)
                        .expireAfter(
                                new Expiry<String, Optional<String>>() {
                                    @Override
                                    public long expireAfterCreate(
                                            String key, Optional<String> value, long now) {
                                        return value.isPresent() ? ttlNanos : negativeTtlNanos;
                                    }

                                    @Override
                                    public long expireAfterUpdate(
                                            String key,
                                            Optional<String> value,
                                            long now,
                                            long currentDuration) {
                                        // A refreshed value gets a fresh TTL.
                                        return expireAfterCreate(key, value, now);
                                    }

                                    @Override
                                    public long expireAfterRead(
                                            String key,
                                            Optional<String> value,
                                            long now,
                                            long currentDuration) {
                                        // Reads don't extend the lifetime.
                                        return currentDuration;
                                    }
                                })
                        .executor(Runnable::run)
                        .ticker(ticker)
                        .recordStats()
                        .build();
    }

    /**
     * Returns the cached value for {@code key}, calling {@code loader} on a miss.
     *
     * @return the value, or {@code null} if the loader reported that none exists
     * @throws Exception whatever the loader threw; nothing is cached in that case
     */
    public String get(String key, Loader loader) throws Exception {
        Optional<String> cached = cache.getIfPresent(key);
        if (cached != null) {
            return cached.orElse(null);
        }
        String loaded = loader.load(key);
        cache.put(key, Optional.ofNullable(loaded));
        return loaded;
    }

    /** One-line summary of size and hit rate, for periodic logging. */
    public String stats() {
        CacheStats s = cache.stats();
        return String.format(
                Locale.ROOT,
                "entries=%d estimatedBytes=%d/%d requests=%d hitRate=%.3f evictions=%d",
                cache.estimatedSize(),
                estimatedBytes(),
                maxBytes,
                s.requestCount(),
                s.hitRate(),
                s.evictionCount());
    }

    /** Estimated bytes held by cached entries, after pending maintenance runs. */
    long estimatedBytes() {
        cache.cleanUp();
        return cache.policy().eviction().map(e -> e.weightedSize().orElse(0L)).orElse(0L);
    }

    /** Drops all entries. */
    public void clear() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    /**
     * Estimates the heap used by one entry. Assumes 2 bytes per char, which overestimates the
     * common case where the JVM stores ASCII strings in 1 byte per char.
     */
    static int estimateBytes(String key, Optional<String> value) {
        long chars = key.length() + value.map(String::length).orElse(0);
        return (int) Math.min(Integer.MAX_VALUE, ENTRY_OVERHEAD_BYTES + 2 * chars);
    }
}
