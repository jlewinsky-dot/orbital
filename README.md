# Satellite Tracking Pipeline

A medallion-style data pipeline that pulls orbital element sets from [CelesTrak](https://celestrak.org/) and turns them into current satellite positions. Built to run on Databricks against a Unity Catalog named `orbital`.

## How it works

The pipeline has three stages, one script per layer:

**Bronze** (`bronze_ingest_celestrak_gp.py`)
Fetches GP (general perturbations) element sets from the CelesTrak API for three groups: Starlink, GPS operational satellites, and space stations. Raw records are appended to `orbital.bronze.celestrak_gp` with the source group and an ingestion timestamp tagged on. Epochs stay as strings here; no cleaning happens at this layer.

**Silver** (`silver_load_gp_history.py`)
Builds `orbital.silver.gp_history`, one row per satellite per epoch. Casts types, filters out rows with missing IDs or epochs and physically impossible values (eccentricity outside [0, 1], non-positive mean motion), and dedupes on (NORAD ID, epoch) keeping the most recently ingested copy. Loads through a MERGE so reruns are idempotent: elements already in the table are skipped, and history accumulates as bronze picks up new epochs.

**Gold** (`gold_build_current_positions.py`)
Takes the latest element set per satellite from silver and propagates each one to run time using SGP4. The TEME position vector gets converted to geocentric latitude, longitude (via GMST rotation), and altitude above mean Earth radius. Results land in `orbital.gold.current_positions`, one row per satellite with its position as of `computed_at`. Satellites that fail to propagate (decayed, bad elements) are dropped.

## Running it

These scripts expect a Databricks notebook or job environment where a `spark` session already exists, so they will not run as plain Python scripts without modification. Run them in order: bronze, then silver, then gold. Each run of bronze appends a fresh snapshot; silver folds new epochs into history; gold fully rewrites the positions table.

The gold notebook installs `sgp4` inline with `%pip install sgp4`. The other dependencies are in `requirements.txt` if you want to set up a local environment for development.

## Notes on accuracy

Positions come from SGP4 over the latest available element set, so accuracy depends on element age. Fresh elements are typically good to a few kilometers; stale ones drift. Latitude is geocentric rather than geodetic, and altitude is measured against a spherical Earth (6371 km mean radius), which is fine for visualization and rough lookups but not for anything operational.
