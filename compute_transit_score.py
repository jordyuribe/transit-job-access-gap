"""
compute_transit_score.py

Computes a tract-level transit access score (0-1) for King County census tracts.
Two components: stop density and route frequency, equally weighted.

Usage:
    python compute_transit_score.py
"""

import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# ── Configuration ────────────────────────────────────────────────────────────

GTFS_DIR = "data/gtfs"
SHAPEFILE_PATH = "data/shapefiles/king_county_displacement_risk.shp"
OUTPUT_DIR = "data/processed"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "transit_scores.geojson")

# ── Helpers ──────────────────────────────────────────────────────────────────

def load_tracts():
    """
    Load King County census tracts from shapefile.
    Reproject to EPSG:32610 (UTM Zone 10N) for accurate area calculation.

    @return GeoDataFrame of census tracts in UTM projection
    """
    print("Loading census tracts...")
    tracts = gpd.read_file(SHAPEFILE_PATH)
    tracts = tracts.to_crs(epsg=32610)
    print(f"  Loaded {len(tracts)} tracts")
    return tracts

def load_stops():
    """
    Load transit stops from GTFS stops.txt and convert to GeoDataFrame.

    @return GeoDataFrame of stops in EPSG:4326
    """
    print("Loading stops...")
    stops = pd.read_csv(os.path.join(GTFS_DIR, "stops.txt"))
    geometry = [Point(lon, lat) for lon, lat in zip(stops["stop_lon"], stops["stop_lat"])]
    stops_gdf = gpd.GeoDataFrame(stops, geometry=geometry, crs="EPSG:4326")
    print(f"  Loaded {len(stops_gdf)} stops")
    return stops_gdf

def get_weekday_trips():
    """
    Identify trip IDs that run on weekday service.
    Checks both calendar.txt and calendar_dates.txt to catch
    all service patterns including exception-based scheduling.

    @return set of trip_id strings representing weekday trips
    """
    print("Identifying weekday trips...")
    trips = pd.read_csv(os.path.join(GTFS_DIR, "trips.txt"))

    # Method 1: calendar.txt — regular weekly schedules
    calendar = pd.read_csv(os.path.join(GTFS_DIR, "calendar.txt"))
    weekday_services = set(calendar[
        (calendar["monday"] == 1) |
        (calendar["tuesday"] == 1) |
        (calendar["wednesday"] == 1) |
        (calendar["thursday"] == 1) |
        (calendar["friday"] == 1)
    ]["service_id"])

    # Method 2: calendar_dates.txt — exception-based scheduling
    # exception_type 1 = service added, 2 = service removed
    calendar_dates = pd.read_csv(os.path.join(GTFS_DIR, "calendar_dates.txt"))
    calendar_dates["date"] = pd.to_datetime(calendar_dates["date"], format="%Y%m%d")
    calendar_dates["weekday"] = calendar_dates["date"].dt.dayofweek  # 0=Mon, 6=Sun

    added_weekday = set(calendar_dates[
        (calendar_dates["exception_type"] == 1) &
        (calendar_dates["weekday"] < 5)
    ]["service_id"])

    all_weekday_services = weekday_services | added_weekday

    weekday_trips = trips[trips["service_id"].isin(all_weekday_services)]["trip_id"]
    trip_set = set(weekday_trips)
    print(f"  Found {len(trip_set)} weekday trips")
    return trip_set

def compute_stop_frequency(weekday_trips):
    """
    Count how many unique weekday trips serve each stop.

    @param weekday_trips: set of weekday trip_id strings
    @return Series mapping stop_id to trip count
    """
    print("Computing stop frequency...")
    stop_times = pd.read_csv(os.path.join(GTFS_DIR, "stop_times.txt"))

    # Filter to weekday trips only
    stop_times = stop_times[stop_times["trip_id"].isin(weekday_trips)]

    # Count unique trips per stop
    frequency = stop_times.groupby("stop_id")["trip_id"].nunique()
    print(f"  Computed frequency for {len(frequency)} stops")
    return frequency

def compute_stop_density(tracts, stops_gdf):
    """
    Spatial join stops to tracts and compute stop density (stops per km2).

    @param tracts: GeoDataFrame of census tracts in UTM projection
    @param stops_gdf: GeoDataFrame of transit stops
    @return Series mapping tract index to stop density
    """
    print("Computing stop density...")

    # Reproject stops to match tracts
    stops_utm = stops_gdf.to_crs(epsg=32610)

    # Spatial join: assign each stop to a tract
    joined = gpd.sjoin(stops_utm, tracts[["geometry", "GEOID"]], how="inner", predicate="within")

    # Count stops per tract
    stop_counts = joined.groupby("GEOID").size().rename("stop_count")

    # Compute tract area in km2
    tracts = tracts.copy()
    tracts["area_km2"] = tracts.geometry.area / 1_000_000

    # Merge and compute density
    tracts = tracts.merge(stop_counts, on="GEOID", how="left")
    tracts["stop_count"] = tracts["stop_count"].fillna(0)
    tracts["stop_density"] = tracts["stop_count"] / tracts["area_km2"]

    return tracts

def compute_tract_frequency(tracts, stops_gdf, frequency):
    """
    Average stop-level trip frequency across all stops in each tract.

    @param tracts: GeoDataFrame of census tracts with GEOID column
    @param stops_gdf: GeoDataFrame of transit stops with stop_id column
    @param frequency: Series mapping stop_id to weekday trip count
    @return GeoDataFrame of tracts with avg_frequency column added
    """
    print("Computing tract-level frequency...")

    stops_utm = stops_gdf.to_crs(epsg=32610)

    # Attach frequency to stops
    stops_utm = stops_utm.copy()
    stops_utm["trip_count"] = stops_utm["stop_id"].map(frequency).fillna(0)

    # Spatial join stops to tracts
    joined = gpd.sjoin(stops_utm[["stop_id", "trip_count", "geometry"]], tracts[["geometry", "GEOID"]], how="inner", predicate="within")

    # Average frequency per tract
    avg_freq = joined.groupby("GEOID")["trip_count"].mean().rename("avg_frequency")

    tracts = tracts.merge(avg_freq, on="GEOID", how="left")
    tracts["avg_frequency"] = tracts["avg_frequency"].fillna(0)

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

def compute_transit_score(tracts):
    """
    Combine stop density and route frequency into a single transit score.
    Both components weighted equally at 50%.

    @param tracts: GeoDataFrame with stop_density and avg_frequency columns
    @return GeoDataFrame with transit_score column added
    """
    print("Computing final transit scores...")
    tracts["density_score"] = normalize(tracts["stop_density"])
    tracts["frequency_score"] = normalize(tracts["avg_frequency"])
    tracts["transit_score"] = (tracts["density_score"] + tracts["frequency_score"]) / 2
    return tracts

def export_results(tracts):
    """
    Export scored tracts to GeoJSON in EPSG:4326 for ArcGIS/web use.

    @param tracts: GeoDataFrame with transit_score and component columns
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Reproject back to standard lat/lon for export
    tracts_export = tracts.to_crs(epsg=4326)

    # Keep only relevant columns
    keep_cols = ["GEOID", "stop_count", "area_km2", "stop_density",
                 "avg_frequency", "density_score", "frequency_score",
                 "transit_score", "geometry"]

    # Only keep columns that exist
    keep_cols = [col for col in keep_cols if col in tracts_export.columns]
    tracts_export = tracts_export[keep_cols]

    tracts_export.to_file(OUTPUT_PATH, driver="GeoJSON")
    print(f"\nExported to {OUTPUT_PATH}")
    print(f"  Tracts scored: {len(tracts_export)}")
    print(f"  Mean transit score: {tracts_export['transit_score'].mean():.3f}")
    print(f"  Min: {tracts_export['transit_score'].min():.3f}  Max: {tracts_export['transit_score'].max():.3f}")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=== compute_transit_score.py ===\n")

    tracts = load_tracts()
    stops_gdf = load_stops()
    weekday_trips = get_weekday_trips()
    frequency = compute_stop_frequency(weekday_trips)
    tracts = compute_stop_density(tracts, stops_gdf)
    tracts = compute_tract_frequency(tracts, stops_gdf, frequency)
    tracts = compute_transit_score(tracts)
    export_results(tracts)

    print("\nDone. Ready for compute_job_proximity.py.")

if __name__ == "__main__":
    main()