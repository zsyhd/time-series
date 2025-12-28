from fastapi import FastAPI, Query, HTTPException
from typing import Optional
import json
import os
from collections import defaultdict

app = FastAPI(
    title="Well Time Series API",
    description="API for well time series data",
    version="3.0.0"
)

# Global cache - بارگذاری فقط یکبار
_cached_data = None

def load_data():
    """Load cleaned data with caching"""
    global _cached_data

    # اگر قبلاً لود شده، همون رو برگردون
    if _cached_data is not None:
        return _cached_data

    # مسیر به فایل JSON (یک پوشه بالاتر از api/)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    json_path = os.path.join(base_dir, "MData_Cleaned.json")

    if not os.path.exists(json_path):
        raise HTTPException(
            status_code=500,
            detail=f"MData_Cleaned.json not found at: {base_dir}"
        )

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            _cached_data = json.load(f)
        return _cached_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading data: {str(e)}")

@app.get("/")
def root():
    """Root endpoint"""
    return {
        "status": "running",
        "version": "3.0.0",
        "endpoints": {
            "/api/well/timeseries": "Get time series data",
            "/api/health": "Health check",
            "/api/stats": "Statistics"
        }
    }

@app.get("/api/health")
def health():
    """Health check"""
    try:
        data = load_data()
        timestamps = [r.get('timestamp') for r in data if r.get('timestamp')]

        return {
            "status": "healthy",
            "records": len(data),
            "cached": _cached_data is not None,
            "time_range": {
                "start": min(timestamps) if timestamps else None,
                "end": max(timestamps) if timestamps else None
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats")
def statistics():
    """Dataset statistics"""
    data = load_data()

    classes = defaultdict(int)
    for r in data:
        classes[r.get('class', 'unknown')] += 1

    sensors = ['p_pdg', 'p_tpt', 't_tpt', 'p_mon_ckp', 't_jus_ckp', 'p_jus_ckgl', 'qgl']
    sensor_stats = {}

    for sensor in sensors:
        values = [r[sensor] for r in data if r.get(sensor) is not None]
        if values:
            sensor_stats[sensor] = {
                "min": round(min(values), 2),
                "max": round(max(values), 2),
                "mean": round(sum(values) / len(values), 2)
            }

    return {
        "total_records": len(data),
        "classes": dict(classes),
        "sensors": sensor_stats
    }

@app.get("/api/well/timeseries")
def timeseries(
    well_id: int = Query(1),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    class_id: Optional[int] = Query(None),
    aggregation: str = Query("minute"),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get time series data"""
    data = load_data()

    # Filter
    filtered = []
    for r in data:
        if r.get('well_id') != well_id:
            continue

        timestamp = r.get('timestamp', '')

        if start_time and timestamp < start_time:
            continue

        if end_time and timestamp > end_time:
            continue

        if class_id is not None and r.get('class') != class_id:
            continue

        filtered.append(r)

    if not filtered:
        return {
            "well_id": well_id,
            "count": 0,
            "points": []
        }

    # Aggregate
    if aggregation == "minute":
        points = []
        for r in filtered[:limit]:
            values = {k: v for k, v in r.items() 
                     if k not in ['timestamp', 'well_id', 'class']}
            points.append({
                "timestamp": r['timestamp'],
                "values": values
            })
    else:
        groups = defaultdict(list)

        for r in filtered:
            ts = r.get('timestamp', '')

            if aggregation == "hour":
                key = ts[:13] + ":00:00"
            elif aggregation == "day":
                key = ts[:10] + " 00:00:00"
            else:
                key = ts

            groups[key].append(r)

        points = []
        for time_key in sorted(groups.keys())[:limit]:
            rows = groups[time_key]

            sensor_vals = defaultdict(list)
            for r in rows:
                for k, v in r.items():
                    if k in ['timestamp', 'well_id', 'class']:
                        continue
                    if v is not None and isinstance(v, (int, float)):
                        sensor_vals[k].append(v)

            aggregated = {
                k: round(sum(v) / len(v), 2)
                for k, v in sensor_vals.items() if v
            }

            points.append({
                "timestamp": time_key,
                "values": aggregated,
                "sample_count": len(rows)
            })

    return {
        "well_id": well_id,
        "aggregation": aggregation,
        "count": len(points),
        "total_filtered": len(filtered),
        "points": points
    }

# برای Vercel - حتماً باید باشه!
handler = app
