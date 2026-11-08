-- Silver reject rules (ROADMAP A.4) as Spark SQL. Same logic and priority as rules.py:
-- the first WHEN that matches gives the reject code; NULL means the row is valid.
-- {view} is a temp view of the standardised DataFrame.
SELECT
  *,
  CASE
    WHEN _raw._corrupt_record IS NOT NULL                       THEN 'MALFORMED_ROW'
    WHEN mobile_class IS NULL                                   THEN 'OUT_OF_SCOPE_MOBILE'
    WHEN ts_utc IS NULL                                         THEN 'BAD_TIMESTAMP'
    WHEN to_date(ts_utc) <> _source_date                        THEN 'TS_OUTSIDE_FILE_DAY'
    WHEN lat = 91.0 OR lon = 181.0                              THEN 'POSITION_SENTINEL'
    WHEN lat IS NULL OR lon IS NULL
      OR lat NOT BETWEEN -90.0 AND 90.0
      OR lon NOT BETWEEN -180.0 AND 180.0                       THEN 'BAD_POSITION'
    WHEN NOT coalesce(trim(_raw.mmsi) RLIKE '^[2-7][0-9]{{8}}$', false) THEN 'BAD_MMSI'
  END AS reject_reason
FROM {view}
