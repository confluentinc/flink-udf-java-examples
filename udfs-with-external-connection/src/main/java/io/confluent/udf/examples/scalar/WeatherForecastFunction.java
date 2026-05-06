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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * A scalar function that fetches weather forecasts from the Open-Meteo API for truck route planning.
 * This function takes a destination's latitude, longitude, and arrival date, then returns the
 * weather forecast as a JSON string.
 *
 * <p>The returned JSON includes daily forecast data:
 * <ul>
 *   <li>temperature_2m_max - Maximum temperature in Celsius</li>
 *   <li>temperature_2m_min - Minimum temperature in Celsius</li>
 *   <li>precipitation_sum - Total precipitation in mm</li>
 *   <li>weather_code - WMO weather interpretation code</li>
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 * CREATE CONNECTION weather_api_connection
 * WITH (
 *   'type' = 'REST',
 *   'endpoint' = 'https://api.open-meteo.com/v1/forecast',
 *   'token' = ''
 * );
 *
 * CREATE FUNCTION get_weather_forecast AS 'io.confluent.udf.examples.scalar.WeatherForecastFunction'
 * USING JAR 'confluent-artifact://your-artifact-id'
 * USING CONNECTIONS(`default`.`yourdb`.`weather_api_connection`);
 *
 * SELECT get_weather_forecast(40.7128, -74.0060, '2024-01-15') FROM your_table;
 * </pre>
 */
public class WeatherForecastFunction extends ScalarFunction {
    private static final Logger LOGGER = LogManager.getLogger();

    private transient String functionUrl = null;
    private transient HttpClient httpClient = null;
    private transient String connectionName = null;

    @Override
    public void open(FunctionContext context) throws Exception {
        connectionName = "weather_api_connection";
        functionUrl = context.getJobParameter(connectionName + ".endpoint", null);
        // Token is optional for Open-Meteo API (it's free and requires no auth)
        // We retrieve it but don't require it
        context.getJobParameter(connectionName + ".token", null);

        if (functionUrl == null) {
            throw new IllegalStateException(
                    "Connection endpoint not found for connection: "
                            + connectionName
                            + ". Make sure to configure the connection with 'endpoint' parameter.");
        }

        this.httpClient =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    /**
     * Fetches the weather forecast for a given location and date.
     *
     * @param latitude the latitude of the destination
     * @param longitude the longitude of the destination
     * @param arrivalDate the arrival date in YYYY-MM-DD format
     * @return the weather forecast as a JSON string
     * @throws IOException if the HTTP request fails
     * @throws InterruptedException if the request is interrupted
     */
    public String eval(Double latitude, Double longitude, String arrivalDate)
            throws IOException, InterruptedException {
        if (latitude == null || longitude == null || arrivalDate == null) {
            return null;
        }

        String requestUrl = String.format(
                "%s?latitude=%f&longitude=%f&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code&start_date=%s&end_date=%s&timezone=auto",
                functionUrl,
                latitude,
                longitude,
                arrivalDate,
                arrivalDate);

        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(requestUrl))
                            .GET()
                            .timeout(Duration.ofSeconds(30))
                            .header("Accept", "application/json")
                            .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new IOException(
                        "HTTP request failed with status code: "
                                + response.statusCode()
                                + ", response: "
                                + response.body());
            }

            return response.body();
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Failed to fetch weather forecast for lat={}, lon={}, date={}: {}",
                    latitude, longitude, arrivalDate, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void close() {
        this.httpClient = null;
        this.functionUrl = null;
        this.connectionName = null;
    }
}
