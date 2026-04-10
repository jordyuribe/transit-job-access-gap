"""
compute_access_gap.py

Merges displacement risk, transit access, and job proximity scores
into a single composite access gap index for King County census tracts.

High access gap = high displacement risk + low transit access + low job proximity.

Usage:
    python compute_access_gap.py
"""

import os
import geopandas as gpd
import pandas as pd

# ── Configuration ────────────────────────────────────────────────────────────

TRANSIT_PATH = "data/processed/transit_scores.geojson"
JOB_PATH = "data/processed/job_proximity_scores.geojson"
DISPLACEMENT_PATH = "data/shapefiles/king_county_displacement_risk.shp"
OUTPUT_DIR = "data/processed"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "access_gap.geojson")

# Top N tracts to flag as high priority
TOP_N = 20

# ── Helpers ──────────────────────────────────────────────────────────────────

def load_transit():
    """
    Load transit access scores from GeoJSON.

    @return GeoDataFrame with GEOID and transit_score columns
    """
    print("Loading transit scores...")
    gdf = gpd.read_file(TRANSIT_PATH)
    gdf["GEOID"] = gdf["GEOID"].astype(str).str.zfill(11)
    print(f"  Loaded {len(gdf)} tracts")
    return gdf[["GEOID", "transit_score", "geometry"]]

def load_jobs():
    """
    Load job proximity scores from GeoJSON.

    @return DataFrame with GEOID and job_proximity_score columns
    """
    print("Loading job proximity scores...")
    gdf = gpd.read_file(JOB_PATH)
    gdf["GEOID"] = gdf["GEOID"].astype(str).str.zfill(11)
    print(f"  Loaded {len(gdf)} tracts")
    return gdf[["GEOID", "job_proximity_score"]]

def load_displacement():
    """
    Load displacement risk scores from Project 1 shapefile.

    @return DataFrame with GEOID and displacement_score columns
    """
    print("Loading displacement risk scores...")
    gdf = gpd.read_file(DISPLACEMENT_PATH)
    gdf["GEOID"] = gdf["GEOID"].astype(str).str.zfill(11)
    print(f"  Loaded {len(gdf)} tracts")

    # Find the displacement score column
    score_col = None
    candidates = ["risk_score", "disp_score", "score", "composite", "displacement_score", "displaceme"]    
    for col in candidates:
        if col in gdf.columns:
            score_col = col
            break

    if score_col is None:
        print(f"  Available columns: {list(gdf.columns)}")
        raise ValueError("Could not find displacement score column. Check column names above.")

    print(f"  Using displacement score column: '{score_col}'")
    gdf = gdf.rename(columns={score_col: "displacement_score"})
    return gdf[["GEOID", "displacement_score"]]

def merge_layers(transit, jobs, displacement):
    """
    Merge all three score layers on GEOID.

    @param transit: GeoDataFrame with transit_score
    @param jobs: DataFrame with job_proximity_score
    @param displacement: DataFrame with displacement_score
    @return GeoDataFrame with all three scores joined
    """
    print("Merging layers...")
    merged = transit.merge(jobs, on="GEOID", how="left")
    merged = merged.merge(displacement, on="GEOID", how="left")

    # Fill any missing values with 0
    merged["transit_score"] = merged["transit_score"].fillna(0)
    merged["job_proximity_score"] = merged["job_proximity_score"].fillna(0)
    merged["displacement_score"] = merged["displacement_score"].fillna(0)

    print(f"  Merged {len(merged)} tracts")
    return merged

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

def compute_access_gap(merged):
    """
    Compute the composite access gap index.
    Formula: displacement_risk + (1 - transit_score) + (1 - job_proximity_score)
    Higher score = more at risk AND less served by transit and jobs.

    @param merged: GeoDataFrame with all three component scores
    @return GeoDataFrame with access_gap_raw and access_gap columns added
    """
    print("Computing access gap index...")

    merged["access_gap_raw"] = (
        merged["displacement_score"] +
        (1 - merged["transit_score"]) +
        (1 - merged["job_proximity_score"])
    )

    merged["access_gap"] = normalize(merged["access_gap_raw"])

    # Flag top N highest gap tracts
    threshold = merged["access_gap"].nlargest(TOP_N).min()
    merged["high_priority"] = merged["access_gap"] >= threshold

    return merged

def print_summary(merged):
    """
    Print top tracts by access gap score.

    @param merged: GeoDataFrame with access_gap column
    """
    print(f"\nAccess Gap Summary:")
    print(f"  Mean access gap score: {merged['access_gap'].mean():.3f}")
    print(f"  Min: {merged['access_gap'].min():.3f}  Max: {merged['access_gap'].max():.3f}")

    print(f"\nTop {TOP_N} highest access gap tracts:")
    top = merged.nlargest(TOP_N, "access_gap")[
        ["GEOID", "displacement_score", "transit_score", "job_proximity_score", "access_gap"]
    ]
    for _, row in top.iterrows():
        print(
            f"  GEOID {row['GEOID']} | "
            f"Displacement: {row['displacement_score']:.2f} | "
            f"Transit: {row['transit_score']:.2f} | "
            f"Jobs: {row['job_proximity_score']:.2f} | "
            f"Gap: {row['access_gap']:.2f}"
        )

def export_results(merged):
    """
    Export final access gap GeoJSON to data/processed/.

    @param merged: GeoDataFrame with all scores and access_gap column
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    merged_export = merged.to_crs(epsg=4326)

    keep_cols = [
        "GEOID", "displacement_score", "transit_score",
        "job_proximity_score", "access_gap_raw", "access_gap",
        "high_priority", "geometry"
    ]
    keep_cols = [col for col in keep_cols if col in merged_export.columns]
    merged_export = merged_export[keep_cols]

    merged_export.to_file(OUTPUT_PATH, driver="GeoJSON")
    print(f"\nExported to {OUTPUT_PATH}")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=== compute_access_gap.py ===\n")

    transit = load_transit()
    jobs = load_jobs()
    displacement = load_displacement()
    merged = merge_layers(transit, jobs, displacement)
    merged = compute_access_gap(merged)
    print_summary(merged)
    export_results(merged)

    print("\nDone. access_gap.geojson is ready for ArcGIS.")

if __name__ == "__main__":
    main()