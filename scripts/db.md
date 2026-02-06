# Database Schema

## Overview

This database contains French railway station nodes enriched with geographic,
administrative, and weather data.

## Tables

### t_nodes

Core table containing railway station information.

**Columns:**

- `id` (INTEGER, PK) - Auto-incrementing technical ID
- `sncf_id` (TEXT, UNIQUE) - SNCF station identifier (e.g.,
  "stop_point:SNCF:87271007:LongDistanceTrain")
- `name` (TEXT) - Station name
- `lat` (REAL) - Latitude
- `lon` (REAL) - Longitude

**Index:** `idx_sncf_id` on `sncf_id`

---

### t_insee

Administrative and demographic data from the French INSEE (National Institute of
Statistics).

**Columns:**

- `id` (INTEGER, PK) - Auto-incrementing technical ID
- `node_id` (INTEGER, FK → t_nodes.id) - Reference to station
- `insee_code` (TEXT) - INSEE commune code
- `city_name` (TEXT) - City name
- `department_code` (TEXT) - Department code (e.g., "13", "75")
- `region_code` (TEXT) - Region code
- `population` (INTEGER) - City population
- `postal_codes` (TEXT) - JSON array of postal codes
- `error_message` (TEXT) - Error if API call failed
- `created_at` (TIMESTAMP) - Record creation time

**Index:** `idx_insee_node_id` on `node_id`

---

### t_weather_station

Weather stations from Météo-France API.

**Columns:**

- `id` (INTEGER, PK) - Auto-incrementing technical ID
- `station_id` (TEXT, UNIQUE) - Météo-France station ID (e.g., "13001003")
- `nom` (TEXT) - Station name
- `department_code` (TEXT) - Department code
- `poste_ouvert` (BOOLEAN) - Whether station is currently operational
- `type_poste` (INTEGER) - Station type
- `lon` (REAL) - Longitude
- `lat` (REAL) - Latitude
- `alt` (INTEGER) - Altitude in meters
- `poste_public` (BOOLEAN) - Whether station is public
- `created_at` (TIMESTAMP) - Record creation time

**Indexes:**

- `idx_station_id` on `station_id`
- `idx_weather_station_coords` on `(lat, lon)`
- `idx_weather_station_dept` on `department_code`

---

### t_weather_data

Monthly weather averages (2020-2025) for each station node.

**Columns:**

- `id` (INTEGER, PK) - Auto-incrementing technical ID
- `node_id` (INTEGER, FK → t_nodes.id) - Reference to station
- `weather_station_id` (INTEGER, FK → t_weather_station.id) - Reference to
  weather station
- `month` (INTEGER) - Month number (1-12)
- `precipitation` (REAL) - Average monthly precipitation in mm (from RR field)
- `average_temp` (REAL) - Average monthly temperature in °C (from TMM field)
- `sunny_days` (REAL) - Average sunny days count (from NBSIGMA80 field)
- `created_at` (TIMESTAMP) - Record creation time

**Unique constraint:** `(node_id, month)`

**Indexes:**

- `idx_weather_data_node` on `node_id`
- `idx_weather_data_station` on `weather_station_id`
- `idx_weather_data_month` on `month`

---

## Relationships

```
t_nodes (1) ──< (N) t_insee
   │
   │
   └──< (N) t_weather_data (N) >── (1) t_weather_station
```

- Each **node** can have one INSEE record (with geographic/administrative data)
- Each **node** has 12 weather_data records (one per month)
- Each **weather_data** record references the closest **weather_station** in the
  same department

---

## Key Queries

### Get station with weather data

```sql
SELECT n.name, n.lat, n.lon, 
       w.month, w.precipitation, w.average_temp, w.sunny_days,
       ws.nom as station_name
FROM t_nodes n
JOIN t_weather_data w ON n.id = w.node_id
JOIN t_weather_station ws ON w.weather_station_id = ws.id
WHERE n.sncf_id = 'stop_point:SNCF:87271007:LongDistanceTrain';
```

### Get all January data

```sql
SELECT n.name, w.precipitation, w.average_temp
FROM t_nodes n
JOIN t_weather_data w ON n.id = w.node_id
WHERE w.month = 1
ORDER BY w.average_temp DESC;
```

### Get station with city info

```sql
SELECT n.name, i.city_name, i.department_code, i.population
FROM t_nodes n
JOIN t_insee i ON n.id = i.node_id
WHERE i.error_message IS NULL;
```
