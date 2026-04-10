"""
compute_job_proximity.py

Computes a tract-level job proximity score (0-1) for King County census tracts.
Uses U.S. Census LEHD/LODES WAC data for job counts at the block level.
Jobs within a 3-mile buffer of each tract centroid are summed and normalized.

Usage:
    python compute_job_proximity.py
"""

import os
import urllib.request
import gzip
import shutil
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from dotenv import load_dotenv

# ── Configuration ────────────────────────────────────────────────────────────

load_dotenv()

LODES_URL = "https://lehd.ces.census.gov/data/lodes/LODES8/wa/wac/wa_wac_S000_JT00_2021.csv.gz"
DATA_DIR = "data/lodes"
GZ_PATH = os.path.join(DATA_DIR, "wa_wac_S000_JT00_2021.csv.gz")
CSV_PATH = os.path.join(DATA_DIR, "wa_wac_S000_JT00_2021.csv")
SHAPEFILE_PATH = "data/shapefiles/king_county_displacement_risk.shp"
OUTPUT_DIR = "data/processed"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "job_proximity_scores.geojson")

# King County FIPS code
KING_COUNTY_FIPS = "53033"

# Buffer distance in meters (~3 miles)
BUFFER_METERS = 4828

# ── Helpers ──────────────────────────────────────────────────────────────────

def download_lodes():
    """
    Download the Washington State LODES WAC file if not already present.
    WAC = Workplace Area Characteristics (jobs by block).
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(CSV_PATH):
        print(f"LODES file already exists at {CSV_PATH} — skipping download.")
        return

    print(f"Downloading LODES WAC data...")
    print(f"  Source: {LODES_URL}")
    urllib.request.urlretrieve(LODES_URL, GZ_PATH)
    print("  Download complete. Extracting...")

    with gzip.open(GZ_PATH, "rb") as f_in:
        with open(CSV_PATH, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"  Extracted to {CSV_PATH}")

def load_king_county_jobs():
    """
    Load LODES WAC data and filter to King County census blocks.
    Block GEOID starts with county FIPS (53033 for King County).

    @return DataFrame with block_id and total job count
    """
    print("Loading LODES job data...")
    df = pd.read_csv(CSV_PATH, dtype={"w_geocode": str})

    # Filter to King County blocks (FIPS 53033)
    df = df[df["w_geocode"].str.startswith(KING_COUNTY_FIPS)]

    # C000 = total jobs
    jobs = df[["w_geocode", "C000"]].copy()
    jobs.columns = ["block_id", "total_jobs"]

    print(f"  King County blocks with jobs: {len(jobs)}")
    print(f"  Total jobs: {jobs['total_jobs'].sum():,}")
    return jobs

def blocks_to_points(jobs):
    """
    Convert census block IDs to point geometries using block centroid lookup.
    Block GEOID encodes lat/lon indirectly — we derive tract GEOID from block ID
    and use the Census geocoder to get block centroids.

    Since block-level shapefiles are large, we aggregate jobs to tract level
    directly using the block GEOID structure:
    - Block GEOID: 15 digits = 2 state + 3 county + 6 tract + 4 block
    - Tract GEOID: first 11 digits of block GEOID

    @param jobs: DataFrame with block_id and total_jobs columns
    @return DataFrame with tract_id and total_jobs aggregated
    """
    print("Aggregating jobs from block to tract level...")

    # Extract tract GEOID from block GEOID (first 11 characters)
    jobs = jobs.copy()
    jobs["tract_id"] = jobs["block_id"].str[:11]

    # Sum jobs per tract
    tract_jobs = jobs.groupby("tract_id")["total_jobs"].sum().reset_index()
    print(f"  Tracts with jobs: {len(tract_jobs)}")
    return tract_jobs

def load_tracts():
    """
    Load King County census tracts and reproject to UTM for distance calculations.

    @return GeoDataFrame of census tracts in EPSG:32610
    """
    print("Loading census tracts...")
    tracts = gpd.read_file(SHAPEFILE_PATH)
    tracts = tracts.to_crs(epsg=32610)
    print(f"  Loaded {len(tracts)} tracts")
    return tracts

def compute_job_proximity(tracts, tract_jobs):
    """
    For each tract, sum all jobs within a 3-mile buffer of its centroid.
    This captures job accessibility beyond just jobs within the tract itself.

    @param tracts: GeoDataFrame of census tracts in UTM projection
    @param tract_jobs: DataFrame with tract_id and total_jobs columns
    @return GeoDataFrame with jobs_within_buffer and job_proximity_score columns
    """
    print("Computing job proximity scores...")

    # Merge job counts onto tracts
    tracts = tracts.copy()

    # Normalize GEOID format for joining
    tracts["GEOID"] = tracts["GEOID"].astype(str).str.zfill(11)
    tract_jobs["tract_id"] = tract_jobs["tract_id"].astype(str).str.zfill(11)

    tracts = tracts.merge(tract_jobs, left_on="GEOID", right_on="tract_id", how="left")
    tracts["total_jobs"] = tracts["total_jobs"].fillna(0)

    # Build a GeoDataFrame of tract centroids with job counts
    job_points = tracts.copy()
    job_points["geometry"] = job_points.geometry.centroid

    # For each tract, buffer its centroid and sum jobs from nearby tract centroids
    print("  Running buffer analysis (this may take a moment)...")

    nearby_jobs = []

    for idx, row in tracts.iterrows():
        buffer = row.geometry.centroid.buffer(BUFFER_METERS)
        # Find all tract centroids within the buffer
        within = job_points[job_points.geometry.within(buffer)]
        nearby_jobs.append(within["total_jobs"].sum())

    tracts["jobs_within_buffer"] = nearby_jobs
    print(f"  Buffer analysis complete.")
    return tracts

def normalize(series):
    """
    Min-max normalize a Series to 0-1 range.

    @param series: pandas Series of numeric values
    @return normalized Series
    """
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val:
        return series * 0
    return (series - min_val) / (max_val - min_val)

def export_results(tracts):
    """
    Export job proximity scores to GeoJSON in EPSG:4326.

    @param tracts: GeoDataFrame with job proximity columns
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    tracts["job_proximity_score"] = normalize(tracts["jobs_within_buffer"])

    tracts_export = tracts.to_crs(epsg=4326)

    keep_cols = ["GEOID", "total_jobs", "jobs_within_buffer",
                 "job_proximity_score", "geometry"]
    keep_cols = [col for col in keep_cols if col in tracts_export.columns]
    tracts_export = tracts_export[keep_cols]

    tracts_export.to_file(OUTPUT_PATH, driver="GeoJSON")
    print(f"\nExported to {OUTPUT_PATH}")
    print(f"  Tracts scored: {len(tracts_export)}")
    print(f"  Mean job proximity score: {tracts_export['job_proximity_score'].mean():.3f}")
    print(f"  Min: {tracts_export['job_proximity_score'].min():.3f}  Max: {tracts_export['job_proximity_score'].max():.3f}")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=== compute_job_proximity.py ===\n")
    download_lodes()
    jobs = load_king_county_jobs()
    tract_jobs = blocks_to_points(jobs)
    tracts = load_tracts()
    tracts = compute_job_proximity(tracts, tract_jobs)
    export_results(tracts)
    print("\nDone. Ready for compute_access_gap.py.")

if __name__ == "__main__":
    main()