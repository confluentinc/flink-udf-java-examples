package io.confluent.udf.examples.cache;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HeapLookupCache}. */
public class HeapLookupCacheTest {

    private static final Duration TTL = Duration.ofMinutes(10);
    private static final Duration NEGATIVE_TTL = Duration.ofMinutes(1);

    private final AtomicLong nanos = new AtomicLong();
    private final AtomicInteger loads = new AtomicInteger();

    private HeapLookupCache newCache(long maxBytes) {
        return new HeapLookupCache(maxBytes, TTL, NEGATIVE_TTL, nanos::get);
    }

    private String countingLoad(String key) {
        loads.incrementAndGet();
        return "value-" + key;
    }

    private void advance(Duration d) {
        nanos.addAndGet(d.toNanos());
    }

    @Test
    public void secondLookupIsServedFromCache() throws Exception {
        HeapLookupCache cache = newCache(1024 * 1024);

        assertThat(cache.get("a", this::countingLoad)).isEqualTo("value-a");
        assertThat(cache.get("a", this::countingLoad)).isEqualTo("value-a");

        assertThat(loads).hasValue(1);
        assertThat(cache.stats()).contains("requests=2", "hitRate=0.500");
    }

    @Test
    public void valueExpiresAfterTtl() throws Exception {
        HeapLookupCache cache = newCache(1024 * 1024);
        cache.get("a", this::countingLoad);

        advance(TTL.minusSeconds(1));
        cache.get("a", this::countingLoad);
        assertThat(loads).hasValue(1);

        advance(Duration.ofSeconds(2));
        cache.get("a", this::countingLoad);
        assertThat(loads).hasValue(2);
    }

    @Test
    public void readsDoNotExtendTtl() throws Exception {
        HeapLookupCache cache = newCache(1024 * 1024);
        cache.get("a", this::countingLoad);

        for (int i = 0; i < 4; i++) {
            advance(TTL.dividedBy(4));
            cache.get("a", this::countingLoad);
        }
        // Read at 1/4, 2/4 and 3/4 of the TTL: hits. Read at 4/4: expired, so a second load.
        assertThat(loads).hasValue(2);
    }

    @Test
    public void notFoundIsCachedForTheShorterNegativeTtl() throws Exception {
        HeapLookupCache cache = newCache(1024 * 1024);
        HeapLookupCache.Loader notFound =
                key -> {
                    loads.incrementAndGet();
                    return null;
                };

        assertThat(cache.get("missing", notFound)).isNull();
        assertThat(cache.get("missing", notFound)).isNull();
        assertThat(loads).hasValue(1);

        advance(NEGATIVE_TTL.plusSeconds(1));
        assertThat(cache.get("missing", notFound)).isNull();
        assertThat(loads).hasValue(2);
    }

    @Test
    public void failuresAreNotCached() throws Exception {
        HeapLookupCache cache = newCache(1024 * 1024);
        HeapLookupCache.Loader failing =
                key -> {
                    loads.incrementAndGet();
                    throw new IOException("service unavailable");
                };

        assertThatThrownBy(() -> cache.get("a", failing))
                .isInstanceOf(IOException.class)
                .hasMessage("service unavailable");

        // The failure left nothing behind, so the next lookup calls the loader again.
        assertThat(cache.get("a", this::countingLoad)).isEqualTo("value-a");
        assertThat(loads).hasValue(2);
    }

    @Test
    public void staysWithinByteBudget() throws Exception {
        String bigValue = "x".repeat(1000);
        int entryBytes = HeapLookupCache.estimateBytes("key-000", Optional.of(bigValue));
        long maxBytes = 10L * entryBytes;
        HeapLookupCache cache = newCache(maxBytes);

        for (int i = 0; i < 100; i++) {
            cache.get(String.format("key-%03d", i), key -> bigValue);
        }

        assertThat(cache.estimatedBytes()).isLessThanOrEqualTo(maxBytes);
        assertThat(cache.stats()).contains("evictions=");
        assertThat(cache.stats()).doesNotContain("evictions=0");
    }

    @Test
    public void estimateGrowsWithContentAndIncludesOverhead() {
        assertThat(HeapLookupCache.estimateBytes("k", Optional.empty()))
                .isEqualTo(HeapLookupCache.ENTRY_OVERHEAD_BYTES + 2);
        assertThat(HeapLookupCache.estimateBytes("k", Optional.of("vvvv")))
                .isEqualTo(HeapLookupCache.ENTRY_OVERHEAD_BYTES + 10);
    }
}
