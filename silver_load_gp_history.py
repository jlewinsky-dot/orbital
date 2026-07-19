"""
silver_load_gp_history.py

Loads orbital.silver.gp_history from orbital.bronze.celestrak_gp.
Grain: one row per satellite per epoch. Idempotent - reruns insert nothing.
"""

CATALOG = "orbital"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.gp_history (
  norad_cat_id      INT       NOT NULL,
  object_name       STRING,
  object_id         STRING,
  epoch             TIMESTAMP NOT NULL,
  mean_motion       DOUBLE,
  eccentricity      DOUBLE,
  inclination       DOUBLE,
  ra_of_asc_node    DOUBLE,
  arg_of_pericenter DOUBLE,
  mean_anomaly      DOUBLE,
  bstar             DOUBLE,
  source_group      STRING,
  ingested_at       TIMESTAMP,
  processed_at      TIMESTAMP
)
""")

spark.sql(f"""
MERGE INTO {CATALOG}.silver.gp_history AS t
USING (
  SELECT
    norad_cat_id, object_name, object_id, epoch,
    mean_motion, eccentricity, inclination,
    ra_of_asc_node, arg_of_pericenter, mean_anomaly, bstar,
    source_group, ingested_at,
    current_timestamp() AS processed_at
  FROM (
    SELECT
      CAST(NORAD_CAT_ID AS INT)        AS norad_cat_id,
      OBJECT_NAME                      AS object_name,
      OBJECT_ID                        AS object_id,
      CAST(EPOCH AS TIMESTAMP)         AS epoch,
      MEAN_MOTION                      AS mean_motion,
      ECCENTRICITY                     AS eccentricity,
      INCLINATION                      AS inclination,
      RA_OF_ASC_NODE                   AS ra_of_asc_node,
      ARG_OF_PERICENTER                AS arg_of_pericenter,
      MEAN_ANOMALY                     AS mean_anomaly,
      BSTAR                            AS bstar,
      _source_group                    AS source_group,
      CAST(_ingested_at AS TIMESTAMP)  AS ingested_at,
      ROW_NUMBER() OVER (
        PARTITION BY NORAD_CAT_ID, EPOCH
        ORDER BY _ingested_at DESC
      ) AS rn
    FROM {CATALOG}.bronze.celestrak_gp
    WHERE NORAD_CAT_ID IS NOT NULL
      AND EPOCH IS NOT NULL
      AND ECCENTRICITY BETWEEN 0 AND 1
      AND MEAN_MOTION > 0
  )
  WHERE rn = 1
) AS s
ON  t.norad_cat_id = s.norad_cat_id
AND t.epoch        = s.epoch
WHEN NOT MATCHED THEN INSERT *
""")