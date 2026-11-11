# Silver model

```mermaid
erDiagram
    VESSEL_POSITION }o--o| DIM_VESSEL : "vessel_sk (as-of ts_utc)"
    VESSEL_POSITION }o--o| DIM_PORT : "port_id"
    REJECTS }o--|| BRONZE_AIS_RAW : "same columns, verbatim"

    VESSEL_POSITION {
        long mmsi PK "natural key 1"
        timestamp ts_utc PK "natural key 2"
        date date "partition"
        int hour
        string mobile_class "A or B"
        double lat
        double lon
        double sog_kn "null if sentinel"
        double cog_deg "null if sentinel"
        double heading_deg "null if sentinel"
        double rot
        string nav_status "controlled label"
        long vessel_sk FK
        string ship_type_group
        string zone "port, anchorage, at_sea"
        string port_id FK
        long gap_s
        double step_km
        double implied_speed_kn
        array flags
    }
    DIM_VESSEL {
        long vessel_sk PK "xxhash64(mmsi, valid_from)"
        long mmsi
        long imo
        string callsign
        string name
        string ship_type
        string ship_type_group
        double length_m
        double width_m
        string attr_hash
        timestamp valid_from
        timestamp valid_to "9999-12-31 if current"
        boolean is_current
    }
    DIM_PORT {
        string port_id PK "UN/LOCODE"
        string name
        double lat
        double lon
        double port_radius_km
        double anchorage_radius_km
    }
    REJECTS {
        string all_26_bronze_columns
        date _source_date "partition"
        string reject_reason
        string _silver_run_id
    }
```

**Grain.** `vessel_position`: one row per `(mmsi, ts_utc)`, guaranteed by conflict resolution
and checked by tests. `dim_vessel`: one row per vessel version; exactly one `is_current` per
mmsi. `dim_port`: one row per port. `rejects`: one row per rejected Bronze row.

**Flow for one date.** Bronze partition → standardise (typed, placeholders to null, labels) →
reject rules (→ `rejects`) → exact dedup → `(mmsi, ts_utc)` conflicts → sentinels →
`dim_vessel` SCD2 merge → movement features (with the previous day's last fix) → zones →
as-of join to `dim_vessel` → `vessel_position` (replaceWhere on `date`).
