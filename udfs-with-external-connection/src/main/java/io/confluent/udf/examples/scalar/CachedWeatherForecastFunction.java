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

package io.confluent.udf.examples.scalar;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import io.confluent.udf.examples.cache.HeapLookupCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;

/**
 * {@link WeatherForecastFunction} with a bounded on-heap cache in front of the external call.
 *
 * <p>Records that resolve to the same cache key reuse one response instead of each making an HTTP
 * call. The key is the destination rounded to {@value #COORDINATE_DECIMALS} decimal places (about 1
 * km, finer than the forecast grid) plus the date. The request is built from the rounded values
 * too, so a cached response always matches its key exactly.
 *
 * <p>Cache behavior:
 *
 * <ul>
 *   <li>HTTP 200: the response is cached for {@link #DEFAULT_TTL}.
 *   <li>HTTP 400 or 404, for example a date outside the forecast range: the function returns {@code
 *       NULL}, and that result is cached for {@link #DEFAULT_NEGATIVE_TTL}.
 *   <li>Any other status, a timeout, or a network error: the function throws and nothing is cached,
 *       so the next record retries.
 * </ul>
 *
 * <p>Hit rate and estimated size are logged every {@link #STATS_LOG_INTERVAL}. See the module
 * README for when caching inside a UDF is and isn't appropriate.
 *
 * <pre>
 * CREATE FUNCTION get_weather_forecast_cached
 *   AS 'io.confluent.udf.examples.scalar.CachedWeatherForecastFunction'
 *   USING JAR 'confluent-artifact://your-artifact-id'
 *   USING CONNECTIONS(`default`.`yourdb`.`weather_api_connection`);
 * </pre>
 */
public class CachedWeatherForecastFunction extends ScalarFunction {
    private static final Logger LOGGER = LogManager.getLogger();

    static final String CONNECTION_NAME = "weather_api_connection";

    /**
     * Upper bound on the heap the cache may use. The UDF process has a fixed memory limit that it
     * shares with the runtime, so keep this small. Exceeding the limit kills the process; it does
     * not throw an exception you can catch.
     */
    static final long DEFAULT_MAX_CACHE_BYTES = 32L * 1024 * 1024;

    /** How long a forecast stays cached. Forecasts change during the day, so keep this short. */
    static final Duration DEFAULT_TTL = Duration.ofMinutes(15);

    /** How long a "no forecast for this input" result stays cached. */
    static final Duration DEFAULT_NEGATIVE_TTL = Duration.ofMinutes(1);

    static final int COORDINATE_DECIMALS = 2;

    private static final Duration STATS_LOG_INTERVAL = Duration.ofMinutes(1);

    private final long maxCacheBytes;
    private final Duration ttl;
    private final Duration negativeTtl;

    private transient String endpoint;
    private transient HttpClient httpClient;
    private transient HeapLookupCache cache;
    private transient long nextStatsLogNanos;

    public CachedWeatherForecastFunction() {
        this(DEFAULT_MAX_CACHE_BYTES, DEFAULT_TTL, DEFAULT_NEGATIVE_TTL);
    }

    /** Visible for testing. */
    CachedWeatherForecastFunction(long maxCacheBytes, Duration ttl, Duration negativeTtl) {
        this.maxCacheBytes = maxCacheBytes;
        this.ttl = ttl;
        this.negativeTtl = negativeTtl;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        endpoint = context.getJobParameter(CONNECTION_NAME + ".endpoint", null);
        if (endpoint == null) {
            throw new IllegalStateException(
                    "Connection endpoint not found for connection: "
                            + CONNECTION_NAME
                            + ". Make sure to configure the connection with 'endpoint' parameter.");
        }
        httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        // Created in open(), not the constructor, so each function instance gets its own cache
        // and it is never serialized with the function.
        cache = new HeapLookupCache(maxCacheBytes, ttl, negativeTtl);
        nextStatsLogNanos = System.nanoTime() + STATS_LOG_INTERVAL.toNanos();
    }

    /**
     * Returns the weather forecast for a location and date as JSON, or {@code NULL} if the API has
     * no forecast for that input.
     *
     * @param latitude the latitude of the destination
     * @param longitude the longitude of the destination
     * @param arrivalDate the arrival date in YYYY-MM-DD format
     */
    public String eval(Double latitude, Double longitude, String arrivalDate) throws Exception {
        if (latitude == null || longitude == null || arrivalDate == null) {
            return null;
        }
        String lat = round(latitude);
        String lon = round(longitude);
        String key = lat + "," + lon + "," + arrivalDate;

        String forecast = cache.get(key, k -> fetchForecast(lat, lon, arrivalDate));

        logStatsPeriodically();
        return forecast;
    }

    private String fetchForecast(String lat, String lon, String date)
            throws IOException, InterruptedException {
        String requestUrl =
                endpoint
                        + "?latitude="
                        + lat
                        + "&longitude="
                        + lon
                        + "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code"
                        + "&start_date="
                        + date
                        + "&end_date="
                        + date
                        + "&timezone=auto";

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(requestUrl))
                        .GET()
                        .timeout(Duration.ofSeconds(30))
                        .header("Accept", "application/json")
                        .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            LOGGER.error(
                    "Failed to fetch weather forecast for lat={}, lon={}, date={}: {}",
                    lat,
                    lon,
                    date,
                    e.getMessage(),
                    e);
            throw e;
        }

        int status = response.statusCode();
        if (status == 200) {
            return response.body();
        }
        if (status == 400 || status == 404) {
            // The API rejected this input. Retrying won't help, so return NULL and let the cache
            // remember that for the negative TTL.
            LOGGER.warn(
                    "No forecast for lat={}, lon={}, date={} (HTTP {}): {}",
                    lat,
                    lon,
                    date,
                    status,
                    response.body());
            return null;
        }
        throw new IOException(
                "HTTP request failed with status code: "
                        + status
                        + ", response: "
                        + response.body());
    }

    private static String round(double coordinate) {
        return String.format(Locale.ROOT, "%." + COORDINATE_DECIMALS + "f", coordinate);
    }

    private void logStatsPeriodically() {
        long now = System.nanoTime();
        if (now - nextStatsLogNanos >= 0) {
            nextStatsLogNanos = now + STATS_LOG_INTERVAL.toNanos();
            LOGGER.info("Weather forecast cache: {}", cache.stats());
        }
    }

    @Override
    public void close() {
        if (cache != null) {
            LOGGER.info("Weather forecast cache at close: {}", cache.stats());
            cache.clear();
        }
        cache = null;
        httpClient = null;
        endpoint = null;
    }
}
