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

## Connection Name Configuration

The functions in this module use hardcoded connection names:
- `WeatherForecastFunction` uses: `weather_api_connection`
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
