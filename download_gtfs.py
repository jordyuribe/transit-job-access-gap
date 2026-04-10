"""
download_gtfs.py

Downloads and extracts the King County Metro GTFS feed into data/gtfs/.
Run this first before any other transit analysis scripts.

Usage:
    python download_gtfs.py
"""

import os
import zipfile
import urllib.request

# ── Configuration ────────────────────────────────────────────────────────────

GTFS_URL = "http://metro.kingcounty.gov/GTFS/google_transit.zip"
DATA_DIR = "data/gtfs"
ZIP_PATH = os.path.join(DATA_DIR, "google_transit.zip")

# ── Helpers ──────────────────────────────────────────────────────────────────

def make_dirs():
    """Create the output directory if it doesn't already exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"Output directory ready: {DATA_DIR}")

def download_feed():
    """
    Download the GTFS ZIP from King County Metro.
    Skips download if the file already exists locally.
    """
    if os.path.exists(ZIP_PATH):
        print(f"ZIP already exists at {ZIP_PATH} — skipping download.")
        return

    print(f"Downloading GTFS feed from:\n  {GTFS_URL}")
    print("This may take a moment (~10 MB)...")

    urllib.request.urlretrieve(GTFS_URL, ZIP_PATH)

    size_mb = os.path.getsize(ZIP_PATH) / (1024 * 1024)
    print(f"Download complete. File size: {size_mb:.1f} MB")

def extract_feed():
    """
    Extract all files from the GTFS ZIP into DATA_DIR.
    Overwrites existing extracted files.
    """
    print(f"Extracting files to {DATA_DIR}/...")

    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        zip_ref.extractall(DATA_DIR)
        extracted = zip_ref.namelist()

    print(f"Extracted {len(extracted)} files:")
    for name in sorted(extracted):
        print(f"  {name}")

def verify_required_files():
    """
    Confirm that the files needed for transit scoring are present.
    Raises an error if any are missing.

    @param none
    @return none
    """
    required = ["stops.txt", "stop_times.txt", "trips.txt", "routes.txt", "calendar.txt"]
    missing = []

    for filename in required:
        full_path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(full_path):
            missing.append(filename)

    if missing:
        raise FileNotFoundError(
            f"Missing required GTFS files: {missing}\n"
            f"Check the ZIP or re-run the download."
        )

    print("\nAll required GTFS files verified.")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=== download_gtfs.py ===\n")
    make_dirs()
    download_feed()
    extract_feed()
    verify_required_files()
    print("\nDone. Ready for compute_transit_score.py.")

if __name__ == "__main__":
    main()