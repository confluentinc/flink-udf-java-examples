package io.confluent.udf.examples.scalar;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.FunctionContext;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CachedWeatherForecastFunction} against a local HTTP server that stands in for
 * the forecast API.
 */
public class CachedWeatherForecastFunctionTest {

    private static final String OK_DATE = "2026-01-15";
    private static final String BAD_DATE = "1900-01-01";
    private static final String BROKEN_DATE = "2000-01-01";

    private final List<String> requests = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private CachedWeatherForecastFunction function;

    @BeforeEach
    public void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext(
                "/v1/forecast",
                exchange -> {
                    String query = exchange.getRequestURI().getQuery();
                    requests.add(query);
                    int status = 200;
                    String body = "{\"query\":\"" + query + "\"}";
                    if (query.contains(BAD_DATE)) {
                        status = 400;
                        body = "{\"error\":true,\"reason\":\"date out of range\"}";
                    } else if (query.contains(BROKEN_DATE)) {
                        status = 500;
                        body = "{\"error\":true}";
                    }
                    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(status, bytes.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(bytes);
                    }
                });
        server.start();

        String endpoint = "http://localhost:" + server.getAddress().getPort() + "/v1/forecast";
        function =
                new CachedWeatherForecastFunction(
                        1024 * 1024, Duration.ofMinutes(15), Duration.ofMinutes(1));
        function.open(contextWith(endpoint));
    }

    @AfterEach
    public void tearDown() {
        function.close();
        server.stop(0);
    }

    private static FunctionContext contextWith(String endpoint) {
        Configuration config =
                Configuration.fromMap(
                        Map.of(
                                CachedWeatherForecastFunction.CONNECTION_NAME + ".endpoint",
                                endpoint));
        return new FunctionContext(
                null, CachedWeatherForecastFunctionTest.class.getClassLoader(), config);
    }

    @Test
    public void nearbyCoordinatesShareOneRequest() throws Exception {
        String first = function.eval(40.71281, -74.00601, OK_DATE);
        // Rounds to the same 2-decimal key, so it is served from the cache.
        String second = function.eval(40.71449, -74.00552, OK_DATE);

        assertThat(second).isEqualTo(first);
        assertThat(requests).hasSize(1);
        // The request uses the rounded coordinates, so the cached response matches its key.
        assertThat(requests.get(0)).contains("latitude=40.71", "longitude=-74.01");
    }

    @Test
    public void differentDateOrLocationIsADifferentRequest() throws Exception {
        function.eval(40.71, -74.01, OK_DATE);
        function.eval(40.71, -74.01, "2026-01-16");
        function.eval(34.05, -118.24, OK_DATE);

        assertThat(requests).hasSize(3);
    }

    @Test
    public void rejectedInputReturnsNullAndIsCached() throws Exception {
        assertThat(function.eval(40.71, -74.01, BAD_DATE)).isNull();
        assertThat(function.eval(40.71, -74.01, BAD_DATE)).isNull();

        assertThat(requests).hasSize(1);
    }

    @Test
    public void serverErrorThrowsAndIsRetried() {
        assertThatThrownBy(() -> function.eval(40.71, -74.01, BROKEN_DATE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("500");
        assertThatThrownBy(() -> function.eval(40.71, -74.01, BROKEN_DATE))
                .isInstanceOf(IOException.class);

        assertThat(requests).hasSize(2);
    }

    @Test
    public void nullInputReturnsNullWithoutARequest() throws Exception {
        assertThat(function.eval(null, -74.01, OK_DATE)).isNull();
        assertThat(function.eval(40.71, null, OK_DATE)).isNull();
        assertThat(function.eval(40.71, -74.01, null)).isNull();

        assertThat(requests).isEmpty();
    }

    @Test
    public void missingConnectionFailsOpen() {
        CachedWeatherForecastFunction unconfigured = new CachedWeatherForecastFunction();
        FunctionContext empty =
                new FunctionContext(null, getClass().getClassLoader(), new Configuration());

        assertThatThrownBy(() -> unconfigured.open(empty))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(CachedWeatherForecastFunction.CONNECTION_NAME);
    }
}
