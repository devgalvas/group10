-- gold.port_congestion_hourly: one row per (port, hour, ship type group).
-- {positions}: Silver vessel_position (gated dates only); {calls}: gold.port_calls.
WITH near_port AS (
  SELECT port_id, date_trunc('HOUR', ts_utc) AS hour_ts, ship_type_group, mmsi, zone, sog_kn
  FROM {positions}
  WHERE zone IN ('port', 'anchorage')
),
occupancy AS (
  SELECT
    port_id, hour_ts, ship_type_group,
    COUNT(DISTINCT CASE WHEN zone = 'port' THEN mmsi END) AS vessels_in_port,
    COUNT(DISTINCT CASE WHEN zone = 'anchorage' AND sog_kn < {anchor_max_sog_kn}
                        THEN mmsi END) AS vessels_at_anchor
  FROM near_port
  GROUP BY port_id, hour_ts, ship_type_group
),
arrivals AS (
  SELECT port_id, date_trunc('HOUR', arrival_ts) AS hour_ts, ship_type_group,
         COUNT(*) AS arrivals
  FROM {calls}
  GROUP BY 1, 2, 3
),
departures AS (
  SELECT port_id, date_trunc('HOUR', departure_ts) AS hour_ts, ship_type_group,
         COUNT(*) AS departures
  FROM {calls}
  GROUP BY 1, 2, 3
),
joined AS (
  SELECT
    coalesce(o.port_id, a.port_id) AS port_id,
    coalesce(o.hour_ts, a.hour_ts) AS hour_ts,
    coalesce(o.ship_type_group, a.ship_type_group) AS ship_type_group,
    coalesce(o.vessels_in_port, 0) AS vessels_in_port,
    coalesce(o.vessels_at_anchor, 0) AS vessels_at_anchor,
    coalesce(a.arrivals, 0) AS arrivals
  FROM occupancy o
  FULL OUTER JOIN arrivals a
    ON o.port_id = a.port_id AND o.hour_ts = a.hour_ts AND o.ship_type_group = a.ship_type_group
)
SELECT
  coalesce(j.port_id, d.port_id) AS port_id,
  coalesce(j.hour_ts, d.hour_ts) AS hour_ts,
  to_date(coalesce(j.hour_ts, d.hour_ts)) AS date,
  coalesce(j.ship_type_group, d.ship_type_group) AS ship_type_group,
  CAST(coalesce(j.vessels_in_port, 0) AS BIGINT) AS vessels_in_port,
  CAST(coalesce(j.vessels_at_anchor, 0) AS BIGINT) AS vessels_at_anchor,
  CAST(coalesce(j.arrivals, 0) AS BIGINT) AS arrivals,
  CAST(coalesce(d.departures, 0) AS BIGINT) AS departures
FROM joined j
FULL OUTER JOIN departures d
  ON j.port_id = d.port_id AND j.hour_ts = d.hour_ts AND j.ship_type_group = d.ship_type_group
