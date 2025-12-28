from fastapi import FastAPI, Query, HTTPException
from typing import Optional
import json
import os
from collections import defaultdict

"""
FastAPI entrypoint for the well time‑series service.

This module is placed inside the ``api`` directory so that Vercel can
automatically detect it as the serverless entrypoint when deploying
the project.  The application exposes several endpoints for health
checks, statistics, and querying minute‑based, hourly or daily
aggregated time‑series data from a pre‑cleaned dataset.  The JSON
file ``MData_Cleaned.json`` is expected to live one directory above
this ``api`` directory.  A fallback to the current working directory
is provided for situations where deployment tools copy the dataset
into the same folder as ``index.py``.

The code uses a simple in‑memory cache to avoid reloading the JSON
file on every request.  If you modify the dataset while the server
is running, restart the server to refresh the cache.
"""

app = FastAPI(
    title="Well Time Series API",
    description="API for well time series data",
    version="3.0.0",
)

# Global cache to ensure the dataset is loaded only once
_cached_data: Optional[list] = None


def load_data() -> list:
    """Load the cleaned dataset with caching.

    The function first checks if the data has already been loaded. If so,
    it returns the cached data. Otherwise it attempts to load the JSON
    file from two possible locations:

    1. ``../MData_Cleaned.json`` relative to this file.
    2. ``./MData_Cleaned.json`` (same directory as ``index.py``).

    If the file is not found in either location, an HTTP 500 error is
    raised.

    Returns:
        list: The loaded dataset as a list of dictionaries.
    """
    global _cached_data

    # Return cached data if already loaded
    if _cached_data is not None:
        return _cached_data

    # Determine the default path (one directory above api/)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    json_path = os.path.join(base_dir, "MData_Cleaned.json")

    # Fallback to current directory if not found
    if not os.path.exists(json_path):
        alt_dir = os.path.dirname(os.path.abspath(__file__))
        alt_path = os.path.join(alt_dir, "MData_Cleaned.json")
        if os.path.exists(alt_path):
            json_path = alt_path
        else:
            # Nothing found → raise informative error
            raise HTTPException(
                status_code=500,
                detail=(
                    f"MData_Cleaned.json not found in expected locations: "
                    f"{json_path} or {alt_path}"
                ),
            )

    # Load JSON file
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            _cached_data = json.load(f)
        return _cached_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading data: {e}")


@app.get("/")
def root():
    """Root endpoint providing basic service information."""
    return {
        "status": "running",
        "version": app.version,
        "endpoints": {
            "/api/well/timeseries": "Get time series data",
            "/api/health": "Health check",
            "/api/stats": "Statistics",
        },
    }


@app.get("/api/health")
def health():
    """Simple health check.

    Returns the number of records, whether the dataset is cached, and
    the time range covered by the dataset.
    """
    try:
        data = load_data()
        timestamps = [r.get("timestamp") for r in data if r.get("timestamp")]

        return {
            "status": "healthy",
            "records": len(data),
            "cached": _cached_data is not None,
            "time_range": {
                "start": min(timestamps) if timestamps else None,
                "end": max(timestamps) if timestamps else None,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats")
def statistics():
    """Return simple statistics about the dataset.

    Computes the distribution of classes and basic statistics (min, max,
    mean) for selected sensors.  You can adjust the list of sensors
    according to your dataset structure.
    """
    data = load_data()

    # Count occurrences of each class
    classes: dict[int, int] = defaultdict(int)
    for r in data:
        classes[r.get("class", "unknown")] += 1

    # Define sensors to compute stats for
    sensors = [
        "p_pdg",
        "p_tpt",
        "t_tpt",
        "p_mon_ckp",
        "t_jus_ckp",
        "p_jus_ckgl",
        "qgl",
    ]
    sensor_stats: dict[str, dict[str, float]] = {}

    for sensor in sensors:
        values = [r[sensor] for r in data if r.get(sensor) is not None]
        if values:
            sensor_stats[sensor] = {
                "min": round(min(values), 2),
                "max": round(max(values), 2),
                "mean": round(sum(values) / len(values), 2),
            }

    return {
        "total_records": len(data),
        "classes": dict(classes),
        "sensors": sensor_stats,
    }


@app.get("/api/well/timeseries")
def timeseries(
    well_id: int = Query(1, description="ID of the well to retrieve data for"),
    start_time: Optional[str] = Query(
        None, description="Filter results starting from this timestamp"
    ),
    end_time: Optional[str] = Query(
        None, description="Filter results up to this timestamp"
    ),
    class_id: Optional[int] = Query(
        None, description="Only include rows matching this class ID"
    ),
    aggregation: str = Query(
        "minute",
        description="Aggregation period: minute (default), hour, or day",
        pattern="^(minute|hour|day)$",
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of aggregated points to return",
    ),
):
    """Return time‑series data for a specific well.

    Data can be filtered by time range and class ID.  Results may be
    aggregated by minute (default), hour or day.  For minute
    aggregation, each data point corresponds directly to a single
    dataset row; for hourly and daily aggregation, values are averaged
    across all rows that fall within the aggregation window.
    """
    data = load_data()

    # Filter by well_id, time range and class
    filtered: list[dict] = []
    for r in data:
        if r.get("well_id") != well_id:
            continue

        timestamp = r.get("timestamp", "")

        if start_time and timestamp < start_time:
            continue

        if end_time and timestamp > end_time:
            continue

        if class_id is not None and r.get("class") != class_id:
            continue

        filtered.append(r)

    # No results
    if not filtered:
        return {
            "well_id": well_id,
            "aggregation": aggregation,
            "count": 0,
            "total_filtered": 0,
            "points": [],
        }

    # Aggregation
    if aggregation == "minute":
        # Return individual points (up to ``limit``)
        points: list[dict] = []
        for r in filtered[:limit]:
            values = {k: v for k, v in r.items() if k not in ["timestamp", "well_id", "class"]}
            points.append({
                "timestamp": r["timestamp"],
                "values": values,
            })
    else:
        # Group by hour or day
        groups: dict[str, list[dict]] = defaultdict(list)

        for r in filtered:
            ts = r.get("timestamp", "")

            if aggregation == "hour":
                key = ts[:13] + ":00:00"
            elif aggregation == "day":
                key = ts[:10] + " 00:00:00"
            else:
                key = ts

            groups[key].append(r)

        # Compute aggregated values
        points = []
        for time_key in sorted(groups.keys())[:limit]:
            rows = groups[time_key]

            sensor_vals: dict[str, list] = defaultdict(list)
            for r in rows:
                for k, v in r.items():
                    if k in ["timestamp", "well_id", "class"]:
                        continue
                    if v is not None and isinstance(v, (int, float)):
                        sensor_vals[k].append(v)

            aggregated: dict[str, float] = {
                k: round(sum(v) / len(v), 2)
                for k, v in sensor_vals.items()
                if v
            }

            points.append({
                "timestamp": time_key,
                "values": aggregated,
                "sample_count": len(rows),
            })

    return {
        "well_id": well_id,
        "aggregation": aggregation,
        "count": len(points),
        "total_filtered": len(filtered),
        "points": points,
    }


# Do not assign a `handler` variable. Vercel will automatically detect the
# FastAPI application via the `app` global. Assigning a handler triggers
# Vercel's BaseHTTPRequestHandler check and results in an `issubclass()`
# TypeError. See deployment guide for details.