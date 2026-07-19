import requests
import json
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

BASE_URL = "https://celestrak.org/NORAD/elements/gp.php"
GROUPS = ["starlink", "gps-ops", "stations"]
FORMAT = "json"

def fetch_group(group: str) -> list[dict]:
    resp = requests.get(
        BASE_URL,
        params={"GROUP": group, "FORMAT": FORMAT},
        timeout=30,
    )

    resp.raise_for_status()
    return resp.json()

if __name__ == "__main__":
    ingest_ts = datetime.now(timezone.utc).isoformat()
    for group in GROUPS:

        records = fetch_group(group)

        for r in records:
            r["_source_group"] = group
            r["_ingested_at"] = ingest_ts
        schema = StructType([
                StructField("OBJECT_NAME", StringType(), True),
                StructField("OBJECT_ID", StringType(), True),
                StructField("NORAD_CAT_ID", IntegerType(), True),
                StructField("EPOCH", StringType(), True),          # keep as string in bronze
                StructField("MEAN_MOTION", DoubleType(), True),
                StructField("ECCENTRICITY", DoubleType(), True),
                StructField("INCLINATION", DoubleType(), True),
                StructField("RA_OF_ASC_NODE", DoubleType(), True),
                StructField("ARG_OF_PERICENTER", DoubleType(), True),
                StructField("MEAN_ANOMALY", DoubleType(), True),
                StructField("BSTAR", DoubleType(), True),
                StructField("_source_group", StringType(), True),
                StructField("_ingested_at", StringType(), True),
            ])
        df = spark.createDataFrame(records, schema=schema)
        df.write.mode("append").saveAsTable("orbital.bronze.celestrak_gp")