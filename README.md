# Transit & Job Access Gap Analysis
### King County, Washington

A spatial equity analysis identifying communities caught in a **double bind** — high displacement risk AND poor access to transit and jobs.

---

## Story Map

[Disconnected: Transit & Job Access Gap in King County](https://storymaps.arcgis.com/stories/50f01148145c40219330de0a83009966)

---

## The Question

Which King County census tracts face the worst combination of:
- High displacement pressure (rising rents, cost burden, low income)
- Low transit access (few stops, infrequent service)
- Low job proximity (few jobs reachable within a reasonable commute)

---

## Key Findings

- **495** King County census tracts scored
- **Mean access gap score: 0.671** — the county is broadly underserved outside the Seattle core
- **Worst tract:** Federal Way (GEOID 53033030007) — displacement 0.79, transit 0.10, jobs 0.03
- **Hot spot corridor:** Federal Way → Kent → Auburn → south King County
- **Seattle core:** lowest gap — well served by transit and employment centers
- **East King County:** uniformly high gap driven by rurality, not displacement

---

## Data Sources

| Dataset | Source | Year |
|---|---|---|
| King County Metro GTFS | metro.kingcounty.gov/GTFS | 2026 |
| U.S. Census LODES WAC | lehd.ces.census.gov | 2021 |
| Displacement Risk Index | Project 1 — seattle-displacement-risk | 2024 ACS |

---

## Pipeline
- download_gtfs.py          → fetches King County Metro GTFS feed
- compute_transit_score.py  → stop density + route frequency → transit score (0–1)
- compute_job_proximity.py  → LODES job data + 3-mile buffer → job proximity score (0–1)
- compute_access_gap.py     → merges all 3 layers → composite access gap index (0–1)

### Composite Formula
access_gap = displacement_score + (1 - transit_score) + (1 - job_proximity_score)

Min-max normalized to 0–1. Higher = more at risk AND less served.

---

## Tech Stack

- **Python** — geopandas, pandas, shapely
- **GTFS** — King County Metro transit schedule data
- **U.S. Census LODES** — block-level employment data
- **ArcGIS Online** — hosted feature layer, choropleth map (Natural Jenks, 5 classes)
- **GitHub Pages** — project hosting

---

## Setup

```bash
git clone https://github.com/jordyuribe/transit-job-access-gap
cd transit-job-access-gap
pip install geopandas pandas shapely python-dotenv
python download_gtfs.py
python compute_transit_score.py
python compute_job_proximity.py
python compute_access_gap.py
```

Data files are excluded from the repo via `.gitignore`. Running the scripts will fetch and generate all required data locally.

---

## Related Project

[Seattle Displacement & Gentrification Risk Map](https://github.com/jordyuribe/seattle-displacement-risk) — the displacement risk layer used in this analysis.

---

## Author

**Jordy Uribe Rivas**
[linkedin.com/in/jordyuribe](https://linkedin.com/in/jordyuribe)
