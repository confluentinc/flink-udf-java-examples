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

package io.confluent.udf.examples.table;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.table.annotation.DataTypeHint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * A table function that geocodes location names using the OpenStreetMap Nominatim API.
 * Returns multiple matching locations with their coordinates.
 *
 * <p>This is useful for truck routing scenarios where you need to convert destination
 * names to coordinates that can then be used with weather forecast functions.
 *
 * <p>Example usage:
 *
 * <pre>
 * CREATE CONNECTION geocoding_api_connection
 * WITH (
 *   'type' = 'REST',
 *   'endpoint' = 'https://nominatim.openstreetmap.org/search',
 *   'token' = ''
 * );
 *
 * CREATE FUNCTION geocode AS 'io.confluent.udf.examples.table.GeocodingTableFunction'
 * USING JAR 'confluent-artifact://your-artifact-id'
 * USING CONNECTIONS(`default`.`yourdb`.`geocoding_api_connection`);
 *
 * SELECT lat, lon, display_name
 * FROM destinations,
 * LATERAL TABLE(geocode(destination_name)) AS T(lat, lon, display_name);
 * </pre>
 */
@FunctionHint(output = @DataTypeHint("ROW<lat DOUBLE, lon DOUBLE, display_name STRING>"))
public class GeocodingTableFunction extends TableFunction<Row> {
    private static final Logger LOGGER = LogManager.getLogger();

    private transient String functionUrl = null;
    private transient HttpClient httpClient = null;
    private transient ObjectMapper objectMapper = null;

    @Override
    public void open(FunctionContext context) throws Exception {
        String connectionName = "geocoding_api_connection";
        functionUrl = context.getJobParameter(connectionName + ".endpoint", null);

        if (functionUrl == null) {
            throw new IllegalStateException(
                    "Connection endpoint not found for connection: "
                            + connectionName
                            + ". Make sure to configure the connection with 'endpoint' parameter.");
        }

        this.httpClient =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Geocodes a location name and emits matching locations as rows.
     *
     * @param locationName the location name to geocode (e.g., "Denver, CO")
     * @throws IOException if the HTTP request fails
     * @throws InterruptedException if the request is interrupted
     */
    public void eval(String locationName) throws IOException, InterruptedException {
        if (locationName == null || locationName.trim().isEmpty()) {
            return;
        }

        String encodedQuery = URLEncoder.encode(locationName, StandardCharsets.UTF_8);
        String requestUrl = functionUrl + "?q=" + encodedQuery + "&format=json&limit=5";

        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(requestUrl))
                            .GET()
                            .timeout(Duration.ofSeconds(30))
                            .header("Accept", "application/json")
                            .header("User-Agent", "FlinkUDF/1.0")
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

            JsonNode results = objectMapper.readTree(response.body());

            if (results.isArray()) {
                for (JsonNode location : results) {
                    double lat = location.path("lat").asDouble();
                    double lon = location.path("lon").asDouble();
                    String displayName = location.path("display_name").asText();

                    collect(Row.of(lat, lon, displayName));
                }
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Failed to geocode location '{}': {}", locationName, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void close() {
        this.httpClient = null;
        this.functionUrl = null;
        this.objectMapper = null;
    }
}
