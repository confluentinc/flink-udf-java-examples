# Flink UDFs with External Connections

This module contains examples of Flink User Defined Functions (UDFs) that use external connection resources to access REST APIs. These UDFs demonstrate how to securely access external services using connection configurations that provide endpoint URLs and authentication tokens.

## Overview

The examples in this module show how to:
- Access external REST APIs from Flink UDFs
- Use connection resources to securely pass credentials
- Implement both scalar and table functions with external connectivity

**Important**: Only `endpoint` and `token` parameters from connection resources can be passed to UDF functions.

## Building the Module

Build the module using Maven:

```shell
cd udfs-with-external-connection
../mvnw clean package
```

This will produce a JAR file at:
```
target/udfs-with-external-connection-1.0.0.jar
```

Upload this JAR to Confluent Cloud as an artifact. More details on uploading artifacts can be found in the [Confluent Cloud documentation](https://docs.confluent.io/cloud/current/flink/how-to-guides/create-udf.html).

## Available Functions

### Scalar Functions

1. **WeatherForecastFunction** - Fetches weather forecasts from Open-Meteo API for truck route planning
2. **CachedWeatherForecastFunction** - The same lookup with a bounded cache on the JVM heap, so repeated inputs don't repeat the HTTP call

### Table Functions

1. **GeocodingTableFunction** - Geocodes location names using OpenStreetMap Nominatim API, returns multiple matching locations

## Complete Examples

### Example 1: Truck Route Weather Forecast (Scalar Function)

This function fetches weather forecasts from the free [Open-Meteo API](https://open-meteo.com/) for truck route planning. It takes a destination's latitude, longitude, and arrival date, then returns the weather forecast as a JSON string.

**Step 1: Create a Test Table**

Create a data generator table that simulates truck route data:

```sql
CREATE TABLE truck_routes (
  truck_id STRING,
  destination_lat DOUBLE,
  destination_lon DOUBLE,
  arrival_date STRING
)
WITH (
  'connector' = 'faker',
  'rows-per-second' = '1',
  'fields.truck_id.expression' = '#{regexify ''TRUCK-[A-Z]{2}[0-9]{4}''}',
  'fields.destination_lat.expression' = '#{Number.randomDouble ''4'',''25'',''48''}',
  'fields.destination_lon.expression' = '#{Number.randomDouble ''4'',''-125'',''-70''}',
  'fields.arrival_date.expression' = '#{date.future ''10'',''DAYS'',''yyyy-MM-dd''}'
);
```

**Important**: Keep throughput low (e.g., 1 row/second) due to limitations of synchronous HTTP calls.

**Step 2: Create the Connection**

```sql
CREATE CONNECTION weather_api_connection
WITH (
  'type' = 'REST',
  'endpoint' = 'https://api.open-meteo.com/v1/forecast',
  'token' = ''
);
```

**Step 3: Create the Function**

```sql
CREATE FUNCTION get_weather_forecast AS 'io.confluent.udf.examples.scalar.WeatherForecastFunction'
USING JAR 'confluent-artifact://your-artifact-id'
USING CONNECTIONS(`default`.`yourdb`.`weather_api_connection`);
```

**Important**: When using the UI to add a function, you need to add the `USING CONNECTIONS` part manually as the UI integration is not ready.

**Note**:
- Replace `your-artifact-id` with the actual artifact ID from Confluent Cloud
- Replace `yourdb` with your database name

**Step 4: Use the Function**

Query with raw JSON response:

```sql
SELECT
  truck_id,
  destination_lat,
  destination_lon,
  arrival_date,
  get_weather_forecast(destination_lat, destination_lon, arrival_date) as weather_forecast
FROM truck_routes;
```

Extract specific fields using `JSON_VALUE`:

```sql
SELECT
  truck_id,
  arrival_date,
  JSON_VALUE(weather_forecast, '$.daily.temperature_2m_max[0]') as high_temp_c,
  JSON_VALUE(weather_forecast, '$.daily.temperature_2m_min[0]') as low_temp_c,
  JSON_VALUE(weather_forecast, '$.daily.precipitation_sum[0]') as precip_mm
FROM (
  SELECT
    truck_id,
    arrival_date,
    get_weather_forecast(destination_lat, destination_lon, arrival_date) as weather_forecast
  FROM truck_routes
);
```

**Returned JSON includes:**
- `daily.time` - the forecast date
- `daily.temperature_2m_max` - high temperature in Celsius
- `daily.temperature_2m_min` - low temperature in Celsius
- `daily.precipitation_sum` - precipitation in mm
- `daily.weather_code` - WMO weather interpretation code

### Example 2: Geocoding Locations (Table Function)

This table function geocodes location names using the free [OpenStreetMap Nominatim API](https://nominatim.openstreetmap.org/). It takes a location name and returns multiple matching locations with their coordinates.

This is useful for converting destination names to coordinates that can then be used with the weather forecast function.

**Step 1: Create a Test Table**

```sql
CREATE TABLE destinations (
  shipment_id STRING,
  destination_name STRING
)
WITH (
  'connector' = 'faker',
  'rows-per-second' = '1',
  'fields.shipment_id.expression' = '#{regexify ''SHIP-[0-9]{6}''}',
  'fields.destination_name.expression' = '#{Address.cityName}'
);
```

**Step 2: Create the Connection**

```sql
CREATE CONNECTION geocoding_api_connection
WITH (
  'type' = 'REST',
  'endpoint' = 'https://nominatim.openstreetmap.org/search',
  'token' = ''
);
```

**Step 3: Create the Function**

```sql
CREATE FUNCTION geocode AS 'io.confluent.udf.examples.table.GeocodingTableFunction'
USING JAR 'confluent-artifact://your-artifact-id'
USING CONNECTIONS(`default`.`yourdb`.`geocoding_api_connection`);
```

**Step 4: Use the Function**

The table function returns multiple rows per input (up to 5 matching locations):

```sql
SELECT
  shipment_id,
  destination_name,
  lat,
  lon,
  display_name
FROM destinations,
LATERAL TABLE(geocode(destination_name)) AS T(lat, lon, display_name);
```

**Combining with Weather Forecast:**

You can chain the geocoding with weather forecasts:

```sql
SELECT
  shipment_id,
  display_name,
  get_weather_forecast(lat, lon, '2024-01-15') as weather
FROM destinations,
LATERAL TABLE(geocode(destination_name)) AS T(lat, lon, display_name);
```

### Example 3: Caching External Lookups on the JVM Heap (Scalar Function)

`CachedWeatherForecastFunction` puts a cache in front of the call that `WeatherForecastFunction` makes. When many records ask for the same forecast, only the first one calls the API. The cache logic lives in [`HeapLookupCache`](src/main/java/io/confluent/udf/examples/cache/HeapLookupCache.java), a small wrapper around [Caffeine](https://github.com/ben-manes/caffeine) that you can reuse for other lookups.

#### When to cache inside a UDF

A UDF cache fits when:
- many records repeat the same lookup key,
- the remote data can be up to one TTL out of date, and
- the cache is only an optimization: the function still returns the right answer when the cache is empty.

It is the wrong tool when you need a complete, consistent copy of reference data. Each function instance has its own cache. The cache is not shared between parallel instances, is not checkpointed, and starts empty after every statement restart. For reference data you join against, bring the data into a Kafka topic (for example with a source connector) and join in Flink SQL. Flink then keeps the data in managed state and makes no call per record.

#### How the cache behaves

| Remote result | Function returns | Cached for |
|---|---|---|
| HTTP 200 | the forecast JSON | `DEFAULT_TTL` (15 minutes) |
| HTTP 400 / 404 (for example, a date outside the forecast range) | `NULL` | `DEFAULT_NEGATIVE_TTL` (1 minute) |
| Any other status, timeout, or network error | throws | not cached, so the next record retries |

- **Bounded by bytes.** The cache evicts once its estimated size reaches `DEFAULT_MAX_CACHE_BYTES` (32 MB). It weighs entries by their string sizes, not by entry count, because response sizes vary.
- **Normalized keys.** Coordinates are rounded to 2 decimal places (about 1 km), which is finer than the forecast grid. The request uses the rounded values too, so every record that maps to a key gets the same response.
- **Fixed lifetime.** Reads don't extend an entry's TTL, so a frequently read entry still refreshes on schedule.
- **No extra threads.** Eviction and expiry run on the calling thread.

#### Memory budget

The UDF runtime has a fixed memory limit, and only part of it is Java heap. The runtime also needs heap and native memory to move records in and out of your function, and it needs more at higher throughput. Keep the cache budget a small fraction of the heap. If the process goes over its memory limit, it is killed instead of seeing an `OutOfMemoryError`, and the statement restarts with empty caches.

To change the budget or TTLs, edit the constants at the top of `CachedWeatherForecastFunction` and rebuild the JAR.

#### Try it

**Step 1: Create a test table with repeating destinations**

Destinations come from a short list, so the cache gets hits:

```sql
CREATE TABLE truck_routes_repeating (
  truck_id STRING,
  destination STRING,
  arrival_date STRING
)
WITH (
  'connector' = 'faker',
  'rows-per-second' = '10',
  'fields.truck_id.expression' = '#{regexify ''TRUCK-[A-Z]{2}[0-9]{4}''}',
  'fields.destination.expression' = '#{Options.option ''40.7128,-74.0060'',''34.0522,-118.2437'',''41.8781,-87.6298'',''29.7604,-95.3698'',''33.4484,-112.0740''}',
  'fields.arrival_date.expression' = '#{date.future ''3'',''DAYS'',''yyyy-MM-dd''}'
);
```

Five destinations and a few dates give about 20 distinct keys. At 10 rows per second the uncached function makes 600 calls a minute; the cached one makes about 20 calls per 15-minute TTL.

**Step 2: Create the function**

This reuses `weather_api_connection` from Example 1:

```sql
CREATE FUNCTION get_weather_forecast_cached
  AS 'io.confluent.udf.examples.scalar.CachedWeatherForecastFunction'
  USING JAR 'confluent-artifact://your-artifact-id'
  USING CONNECTIONS(`default`.`yourdb`.`weather_api_connection`);
```

**Step 3: Use the function**

```sql
SELECT
  truck_id,
  destination,
  arrival_date,
  get_weather_forecast_cached(
    CAST(SPLIT_INDEX(destination, ',', 0) AS DOUBLE),
    CAST(SPLIT_INDEX(destination, ',', 1) AS DOUBLE),
    arrival_date) AS weather_forecast
FROM truck_routes_repeating;
```

**Step 4: Check the hit rate**

The function logs a summary every minute and when it closes:

```
Weather forecast cache: entries=20 estimatedBytes=24800/33554432 requests=600 hitRate=0.967 evictions=0
```

A low `hitRate` means records rarely repeat a key, so the cache costs memory without saving calls. Rising `evictions` while `estimatedBytes` sits at the limit means the budget is too small for the key space.

## Connection Name Configuration

The functions in this module use hardcoded connection names:
- `WeatherForecastFunction` uses: `weather_api_connection`
- `CachedWeatherForecastFunction` uses: `weather_api_connection`
- `GeocodingTableFunction` uses: `geocoding_api_connection`

To use different connection names, you can either:
1. Create connections with these exact names, or
2. Modify the source code to use your preferred connection names

The connection name is used to retrieve parameters via `context.getJobParameter(connectionName + ".endpoint", null)` and `context.getJobParameter(connectionName + ".token", null)`.

## Performance Considerations

- **Synchronous Calls**: These functions make synchronous HTTP calls, which can impact throughput
- **Rate Limiting**: Keep data generation rates low (1-10 rows/second) when testing. Nominatim has strict rate limits.
- **Timeout Settings**: Functions are configured with 30-second timeouts for HTTP requests
- **Connection Reuse**: The `HttpClient` is created once in the `open()` method and reused for all requests
- **Caching**: If records repeat lookup keys, a bounded heap cache avoids repeated calls (see Example 3)

## Error Handling

All functions will throw `IOException` if:
- The HTTP request fails (non-200 status codes)
- The connection endpoint is not configured
- Network timeouts occur

Make sure to handle these errors appropriately in your Flink SQL queries or job configuration.

## Additional Resources

- [Confluent Cloud UDF Documentation](https://docs.confluent.io/cloud/current/flink/how-to-guides/create-udf.html)
- [Apache Flink UDF Documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/functions/udfs/)
- [Open-Meteo API Documentation](https://open-meteo.com/en/docs)
- [Nominatim API Documentation](https://nominatim.org/release-docs/develop/api/Search/)
