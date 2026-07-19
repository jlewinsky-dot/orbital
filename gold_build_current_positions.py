"""
gold_build_current_positions.py

Builds orbital.gold.current_positions: one row per satellite with its
propagated position (lat/lon/alt) as of run time, using SGP4 over the
latest element set in silver.gp_history.
"""
%pip install sgp4
import math
from datetime import datetime, timezone

from sgp4.api import Satrec, WGS72, jday

CATALOG = "orbital"

# ── 1. latest element set per satellite ──────────────────────────────
latest = spark.sql(f"""
    SELECT * FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY norad_cat_id
               ORDER BY epoch DESC
             ) AS rn
      FROM {CATALOG}.silver.gp_history
    )
    WHERE rn = 1
""").drop("rn").toPandas()

# 2. propagate each satellite to "now"
now = datetime.now(timezone.utc)
jd, fr = jday(now.year, now.month, now.day, now.hour, now.minute,
              now.second + now.microsecond / 1e6)

DEG2RAD = math.pi / 180.0
GMST_RAD = (
    # Greenwich sidereal angle for TEME→earth-fixed longitude
    (280.46061837 + 360.98564736629 * ((jd + fr) - 2451545.0)) % 360.0
) * DEG2RAD

rows = []
for r in latest.itertuples():
    epoch_dt = r.epoch.to_pydatetime().replace(tzinfo=timezone.utc)
    epoch_days = (epoch_dt - datetime(1949, 12, 31, tzinfo=timezone.utc)).total_seconds() / 86400.0

    sat = Satrec()
    sat.sgp4init(
        WGS72, "i", int(r.norad_cat_id), epoch_days,
        r.bstar, 0.0, 0.0, # bstar, ndot, nddot
        r.eccentricity,
        r.arg_of_pericenter * DEG2RAD,
        r.inclination * DEG2RAD,
        r.mean_anomaly * DEG2RAD,
        r.mean_motion * 2.0 * math.pi / 1440.0, # rev/day → rad/min
        r.ra_of_asc_node * DEG2RAD,
    )

    err, pos, _ = sat.sgp4(jd, fr) # pos = (x, y, z) km in TEME frame
    if err != 0:
        continue # propagation failed (decayed, bad elements)

    x, y, z = pos
    r_xy = math.hypot(x, y)
    lat = math.degrees(math.atan2(z, r_xy)) # geocentric lat
    lon = (math.degrees(math.atan2(y, x) - GMST_RAD) + 540) % 360 - 180
    alt_km = math.sqrt(x*x + y*y + z*z) - 6371.0 # above mean radius

    rows.append((
        int(r.norad_cat_id), r.object_name, r.source_group,
        epoch_dt, lat, lon, alt_km, now,
    ))

# 3. land the gold table
gold_df = spark.createDataFrame(
    rows,
    schema=("norad_cat_id INT, object_name STRING, source_group STRING, "
            "element_epoch TIMESTAMP, latitude DOUBLE, longitude DOUBLE, "
            "altitude_km DOUBLE, computed_at TIMESTAMP"),
)

gold_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.gold.current_positions")

print(f"current_positions: {gold_df.count()} satellites propagated at {now.isoformat()}")