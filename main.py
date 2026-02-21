import hashlib
import math
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="NOMADS HRRR GRIB2->JSON Wrapper", version="0.1.0")

NOMADS_BASE = "https://nomads.ncep.noaa.gov"
CACHE_DIR = Path(os.getenv("CACHE_DIR", "/tmp/nomads_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Regex patterns against wgrib2 inventory (-s) text.
# You can tune these after checking /debug/hrrr_inventory.
VAR_PATTERNS = {
    "u10": [r":UGRD:10 m above ground:"],
    "v10": [r":VGRD:10 m above ground:"],
    "gust_10m": [r":GUST:surface:"],
    "mslp": [r":PRMSL:mean sea level:", r":MSLET:mean sea level:"],
    "u925": [r":UGRD:925 mb:"],
    "v925": [r":VGRD:925 mb:"],
    "u850": [r":UGRD:850 mb:"],
    "v850": [r":VGRD:850 mb:"],
    "z850": [r":HGT:850 mb:"],
    # Optional; may need tuning based on actual HRRR inventory
    "cape": [r":CAPE:"],
    "cin": [r":CIN:"],
    "lifted_index": [r":LFTX:", r":LFTX:surface:"],
    "pbl_height": [r":HPBL:"],
}

# Keep MVP subset compact: common wind + pressure + low-level flow + optional stability fields
BASE_HRRR_FLAGS = {
    # levels (surface-only MVP)
    "lev_surface": "on",
    "lev_mean_sea_level": "on",
    "lev_10_m_above_ground": "on",

    # core vars
    "var_UGRD": "on",
    "var_VGRD": "on",
    "var_GUST": "on",
    "var_PRMSL": "on",
    # If this causes issues, leave only PRMSL first:
    "var_MSLET": "on",
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_fhrs(fhrs: str) -> List[int]:
    vals: List[int] = []
    for p in fhrs.split(","):
        p = p.strip()
        if not p:
            continue
        vals.append(int(p))
    if not vals:
        raise ValueError("No forecast hours provided")
    return vals


def compute_meteorological_dir_deg(u: float, v: float) -> float:
    # Meteorological direction = where wind is FROM, clockwise from north
    return (270.0 - math.degrees(math.atan2(v, u))) % 360.0


def choose_hrrr_cycle(run_utc: Optional[str]) -> Tuple[str, int]:
    """
    Returns (YYYYMMDD, cycle_hour_utc).
    If run_utc omitted, chooses a likely available latest cycle using a safety lag.
    Accepts YYYYMMDDHH or ISO8601 string.
    """
    if run_utc:
        s = run_utc.strip()
        if re.fullmatch(r"\d{10}", s):
            dt = datetime.strptime(s, "%Y%m%d%H").replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        return dt.strftime("%Y%m%d"), dt.hour

    dt = now_utc() - timedelta(hours=1, minutes=15)
    return dt.strftime("%Y%m%d"), dt.hour


def hrrr_file_name(cycle_hour: int, fhr: int) -> str:
    return f"hrrr.t{cycle_hour:02d}z.wrfsfcf{fhr:02d}.grib2"


def hrrr_dir(date_yyyymmdd: str) -> str:
    return f"/hrrr.{date_yyyymmdd}/conus"


def nomads_hrrr_url(lat: float, lon: float, cycle_date: str, cycle_hour: int, fhr: int, bbox_deg: float = 0.12) -> str:
    # Small bbox around point reduces download size; wgrib2 still extracts nearest gridpoint
    leftlon = lon - bbox_deg
    rightlon = lon + bbox_deg
    bottomlat = lat - bbox_deg
    toplat = lat + bbox_deg

    params = {
        **BASE_HRRR_FLAGS,
        "subregion": "",
        "leftlon": f"{leftlon:.4f}",
        "rightlon": f"{rightlon:.4f}",
        "toplat": f"{toplat:.4f}",
        "bottomlat": f"{bottomlat:.4f}",
        "dir": hrrr_dir(cycle_date),
        "file": hrrr_file_name(cycle_hour, fhr),
    }

    req = requests.Request("GET", f"{NOMADS_BASE}/cgi-bin/filter_hrrr_2d.pl", params=params).prepare()
    return req.url


def cache_path_for_url(url: str) -> Path:
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{h}.grib2"


def download_with_cache(url: str, force_refresh: bool = False) -> Path:
    dest = cache_path_for_url(url)
    if dest.exists() and not force_refresh and dest.stat().st_size > 0:
        return dest

    tmp = dest.with_suffix(".tmp")
    with requests.get(url, stream=True, timeout=(10, 90)) as r:
        r.raise_for_status()
        ctype = (r.headers.get("content-type") or "").lower()
        if "text/html" in ctype:
            body = r.text[:1200]
            raise HTTPException(status_code=502, detail=f"NOMADS returned HTML error page: {body}")

        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 64):
                if chunk:
                    f.write(chunk)

    tmp.replace(dest)

    if dest.stat().st_size == 0:
        raise HTTPException(status_code=502, detail="Downloaded GRIB2 file was empty")
    return dest


def run_wgrib2(args: List[str]) -> str:
    cmd = ["wgrib2"] + args
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=45)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="wgrib2 not installed in container")
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="wgrib2 timed out")

    if proc.returncode != 0:
        raise HTTPException(status_code=500, detail=f"wgrib2 failed: {proc.stderr[:2000]}")
    return proc.stdout


def grib_inventory(grib_file: Path) -> str:
    return run_wgrib2([str(grib_file), "-s"])


def lon_to_0_360(lon: float) -> float:
    return lon % 360.0


def parse_lon_output_val(line: str) -> Optional[float]:
    m = re.search(r"val=([-+0-9.eE]+)", line)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def extract_first_match_value(grib_file: Path, regex_pattern: str, lat: float, lon: float) -> Optional[float]:
    lon360 = lon_to_0_360(lon)
    # -match filters records; -lon outputs nearest-gridpoint value(s)
    out = run_wgrib2([str(grib_file), "-s", "-match", regex_pattern, "-lon", f"{lon360}", f"{lat}"])
    vals = []
    for ln in out.splitlines():
        v = parse_lon_output_val(ln)
        if v is not None:
            vals.append(v)
    return vals[-1] if vals else None


def extract_fields(grib_file: Path, lat: float, lon: float) -> Dict[str, Optional[float]]:
    result: Dict[str, Optional[float]] = {}

    for key, patterns in VAR_PATTERNS.items():
        value = None
        for pat in patterns:
            try:
                value = extract_first_match_value(grib_file, pat, lat, lon)
            except HTTPException:
                value = None
            if value is not None:
                break
        result[key] = value

    u10 = result.get("u10")
    v10 = result.get("v10")
    if u10 is not None and v10 is not None:
        result["wind_speed_10m"] = math.hypot(u10, v10)  # m/s
        result["wind_dir_10m"] = compute_meteorological_dir_deg(u10, v10)
    else:
        result["wind_speed_10m"] = None
        result["wind_dir_10m"] = None

    return result


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/debug/hrrr_inventory")
def debug_hrrr_inventory(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    fhr: int = Query(0, ge=0, le=48),
    run_utc: Optional[str] = Query(None, description="YYYYMMDDHH or ISO8601 Z"),
    force_refresh: bool = Query(False),
):
    cycle_date, cycle_hour = choose_hrrr_cycle(run_utc)
    url = nomads_hrrr_url(lat, lon, cycle_date, cycle_hour, fhr)
    grib_file = download_with_cache(url, force_refresh=force_refresh)
    inv = grib_inventory(grib_file)

    return JSONResponse(
        {
            "meta": {
                "source": "nomads",
                "model": "hrrr",
                "run_utc": f"{cycle_date}{cycle_hour:02d}",
                "fhr": fhr,
                "nomads_url": url,
            },
            "inventory": inv.splitlines(),
        }
    )


@app.get("/hrrr/point")
def hrrr_point(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    fhrs: str = Query("0,1,2,3,4,5"),
    run_utc: Optional[str] = Query(None, description="YYYYMMDDHH or ISO8601 Z"),
    units: str = Query("knots", pattern="^(si|knots)$"),
    force_refresh: bool = Query(False),
):
    try:
        fh_list = parse_fhrs(fhrs)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    cycle_date, cycle_hour = choose_hrrr_cycle(run_utc)
    run_dt = datetime.strptime(f"{cycle_date}{cycle_hour:02d}", "%Y%m%d%H").replace(tzinfo=timezone.utc)

    hourly: Dict[str, List[Optional[float]]] = {
        "time": [],
        "u10": [],
        "v10": [],
        "wind_speed_10m": [],
        "wind_dir_10m": [],
        "gust_10m": [],
        "mslp": [],
        "u925": [],
        "v925": [],
        "u850": [],
        "v850": [],
        "z850": [],
        "cape": [],
        "cin": [],
        "lifted_index": [],
        "pbl_height": [],
    }

    urls_used: List[str] = []

    for fhr in fh_list:
        url = nomads_hrrr_url(lat, lon, cycle_date, cycle_hour, fhr)
        urls_used.append(url)
        grib_file = download_with_cache(url, force_refresh=force_refresh)
        fields = extract_fields(grib_file, lat, lon)

        valid_dt = run_dt + timedelta(hours=fhr)
        hourly["time"].append(valid_dt.isoformat().replace("+00:00", "Z"))

        # Vector components remain SI (m/s) in this MVP
        hourly["u10"].append(fields.get("u10"))
        hourly["v10"].append(fields.get("v10"))
        hourly["u925"].append(fields.get("u925"))
        hourly["v925"].append(fields.get("v925"))
        hourly["u850"].append(fields.get("u850"))
        hourly["v850"].append(fields.get("v850"))

        # Scalars
        hourly["wind_dir_10m"].append(fields.get("wind_dir_10m"))
        hourly["mslp"].append(fields.get("mslp"))
        hourly["z850"].append(fields.get("z850"))
        hourly["cape"].append(fields.get("cape"))
        hourly["cin"].append(fields.get("cin"))
        hourly["lifted_index"].append(fields.get("lifted_index"))
        hourly["pbl_height"].append(fields.get("pbl_height"))

        ws = fields.get("wind_speed_10m")
        gust = fields.get("gust_10m")

        if units == "knots":
            hourly["wind_speed_10m"].append(ws * 1.943844492 if ws is not None else None)
            hourly["gust_10m"].append(gust * 1.943844492 if gust is not None else None)
        else:
            hourly["wind_speed_10m"].append(ws)
            hourly["gust_10m"].append(gust)

    return {
        "meta": {
            "source": "nomads",
            "model": "hrrr",
            "run_utc": f"{cycle_date}{cycle_hour:02d}",
            "cycle_hour": cycle_hour,
            "units": units,
            "notes": [
                "GRIB2 decoded with wgrib2 nearest-gridpoint extraction.",
                "Some optional fields may return null until VAR_PATTERNS are tuned using /debug/hrrr_inventory."
            ],
        },
        "location": {"lat": lat, "lon": lon},
        "hourly": hourly,
        "debug": {
            "nomads_urls": urls_used
        },
    }