import hashlib
import math
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="NOMADS HRRR GRIB2->JSON Wrapper", version="0.3.0")

NOMADS_BASE = "https://nomads.ncep.noaa.gov"
CACHE_DIR = Path(os.getenv("CACHE_DIR", "/tmp/nomads_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Field extraction patterns (wgrib2 inventory text) ----------
# Keep broad pattern variants; missing fields simply return null.
VAR_PATTERNS: Dict[str, List[str]] = {
    # Wind vectors / pressure
    "u10": [r":UGRD:10 m above ground:"],
    "v10": [r":VGRD:10 m above ground:"],
    "gust_10m": [r":GUST:surface:"],
    "mslp": [r":MSLET:mean sea level:", r":PRMSL:mean sea level:"],

    # Pressure-level winds / heights / temps
    "u925": [r":UGRD:925 mb:"],
    "v925": [r":VGRD:925 mb:"],
    "u850": [r":UGRD:850 mb:"],
    "v850": [r":VGRD:850 mb:"],
    "z850": [r":HGT:850 mb:"],
    "temp_850_k": [r":TMP:850 mb:"],

    # Surface / boundary layer / stability
    "temp_2m_k": [r":TMP:2 m above ground:"],
    "cape": [r":CAPE:"],
    "cin": [r":CIN:"],
    "lifted_index": [r":LFTX:", r":LFTX:surface:"],
    "pbl_height": [r":HPBL:"],

    # Cloud cover (percent) - pattern variants are intentionally broad
    "cloud_cover_total": [r":TCDC:entire atmosphere", r":TCDC:"],
    "cloud_cover_low": [r":LCDC:low cloud layer", r":LCDC:"],
    "cloud_cover_mid": [r":MCDC:middle cloud layer", r":MCDC:"],
    "cloud_cover_high": [r":HCDC:high cloud layer", r":HCDC:"],
}

# ---------- HRRR bundle configs ----------
# Split into multiple requests so one unsupported var/level combo doesn't break everything.
# surface_core is required; all others are best-effort optional.
BUNDLE_CONFIGS: Dict[str, Dict[str, Any]] = {
    "surface_core": {
        "product": "wrfsfcf",
        "flags": {
            "lev_10_m_above_ground": "on",
            "var_UGRD": "on",
            "var_VGRD": "on",
        },
        "required_for_point": True,
    },
    "surface_diag": {
        "product": "wrfsfcf",
        "flags": {
            "lev_surface": "on",
            "lev_mean_sea_level": "on",
            "lev_2_m_above_ground": "on",
            "var_GUST": "on",
            "var_MSLET": "on",
            "var_TMP": "on",
        },
        "required_for_point": False,
    },
    "pbl": {
        "product": "wrfsfcf",
        "flags": {
            "lev_surface": "on",
            "var_HPBL": "on",
        },
        "required_for_point": False,
    },
    "clouds": {
        "product": "wrfsfcf",
        "flags": {
            "lev_entire_atmosphere": "on",
            "lev_low_cloud_layer": "on",
            "lev_middle_cloud_layer": "on",
            "lev_high_cloud_layer": "on",
            "var_TCDC": "on",
            "var_LCDC": "on",
            "var_MCDC": "on",
            "var_HCDC": "on",
        },
        "required_for_point": False,
    },
    "thermo_stability": {
        "product": "wrfsfcf",
        "flags": {
            "lev_surface": "on",
            "var_CAPE": "on",
            "var_CIN": "on",
            "var_LFTX": "on",
        },
        "required_for_point": False,
    },
    "pressure_low": {
        "product": "wrfprsf",
        "flags": {
            "lev_925_mb": "on",
            "lev_850_mb": "on",
            "var_UGRD": "on",
            "var_VGRD": "on",
            "var_HGT": "on",
            "var_TMP": "on",
        },
        "required_for_point": False,
    },
}

OPTIONAL_BUNDLE_ORDER = ["surface_diag", "pbl", "clouds", "thermo_stability", "pressure_low"]

ALL_HOURLY_KEYS = [
    "time",

    # raw / direct values
    "u10",
    "v10",
    "wind_speed_10m",
    "wind_dir_10m",
    "gust_10m",
    "mslp",

    "u925",
    "v925",
    "wind_speed_925",
    "wind_dir_925",

    "u850",
    "v850",
    "z850",

    "cape",
    "cin",
    "lifted_index",
    "pbl_height",

    "cloud_cover_total",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",

    "temp_2m_c",
    "temp_850_c",
    "temp_diff_2m_850_c",

    # derived regime features
    "surface_925_wind_diff",
    "surface_925_dir_diff_deg",
    "shear_0_1km_proxy_10m_925",
    "mixing_efficiency_ratio_10m_to_925",
]


class NomadsDownloadError(Exception):
    """Raised when NOMADS returns a bad response / missing file / network issue."""

    def __init__(
        self,
        message: str,
        *,
        url: str,
        status_code: Optional[int] = None,
        content_type: Optional[str] = None,
        body_preview: Optional[str] = None,
    ):
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.content_type = content_type
        self.body_preview = body_preview

    def short(self) -> str:
        parts = [str(self)]
        if self.status_code is not None:
            parts.append(f"status={self.status_code}")
        if self.content_type:
            parts.append(f"content_type={self.content_type}")
        return "; ".join(parts)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_run_utc_to_dt(run_utc: str) -> datetime:
    s = run_utc.strip()
    if re.fullmatch(r"\d{10}", s):
        return datetime.strptime(s, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def auto_start_cycle_dt() -> datetime:
    # Safety lag to avoid selecting a cycle before files fully appear
    return now_utc() - timedelta(hours=1, minutes=15)


def cycle_dt_to_parts(dt: datetime) -> Tuple[str, int]:
    return dt.strftime("%Y%m%d"), dt.hour


def parse_fhrs(fhrs: str) -> List[int]:
    vals: List[int] = []
    for p in fhrs.split(","):
        p = p.strip()
        if not p:
            continue
        vals.append(int(p))
    if not vals:
        raise ValueError("No forecast hours provided")
    if any(v < 0 for v in vals):
        raise ValueError("Forecast hours must be >= 0")
    return vals


def compute_meteorological_dir_deg(u: float, v: float) -> float:
    # Meteorological direction = where wind is FROM, clockwise from north
    return (270.0 - math.degrees(math.atan2(v, u))) % 360.0


def angle_diff_deg(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    d = (b - a + 180.0) % 360.0 - 180.0
    return abs(d)


def k_to_c(k: Optional[float]) -> Optional[float]:
    if k is None:
        return None
    return k - 273.15


def hrrr_file_name(cycle_hour: int, fhr: int, product: str) -> str:
    # product examples: wrfsfcf, wrfprsf
    return f"hrrr.t{cycle_hour:02d}z.{product}{fhr:02d}.grib2"


def hrrr_dir(date_yyyymmdd: str) -> str:
    return f"/hrrr.{date_yyyymmdd}/conus"


def nomads_hrrr_url(
    lat: float,
    lon: float,
    cycle_date: str,
    cycle_hour: int,
    fhr: int,
    *,
    product: str,
    flags: Dict[str, str],
    bbox_deg: float = 0.12,
) -> str:
    # small bbox around point to reduce file size
    leftlon = lon - bbox_deg
    rightlon = lon + bbox_deg
    bottomlat = lat - bbox_deg
    toplat = lat + bbox_deg

    params = {
        **flags,
        "subregion": "",
        "leftlon": f"{leftlon:.4f}",
        "rightlon": f"{rightlon:.4f}",
        "toplat": f"{toplat:.4f}",
        "bottomlat": f"{bottomlat:.4f}",
        "dir": hrrr_dir(cycle_date),
        "file": hrrr_file_name(cycle_hour, fhr, product),
    }

    req = requests.Request("GET", f"{NOMADS_BASE}/cgi-bin/filter_hrrr_2d.pl", params=params).prepare()
    return req.url


def cache_path_for_url(url: str) -> Path:
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{h}.grib2"


def download_with_cache(url: str, force_refresh: bool = False) -> Path:
    """
    Download NOMADS subset response and cache it.
    Raises NomadsDownloadError with detailed context on failure.
    """
    dest = cache_path_for_url(url)
    if dest.exists() and not force_refresh and dest.stat().st_size > 0:
        return dest

    tmp = dest.with_suffix(".tmp")

    try:
        with requests.get(url, stream=True, timeout=(10, 90)) as r:
            status = r.status_code
            ctype = (r.headers.get("content-type") or "").lower()

            if status != 200:
                try:
                    body_preview = r.text[:1500]
                except Exception:
                    body_preview = "<unable to read response body>"
                raise NomadsDownloadError(
                    "NOMADS non-200 response",
                    url=url,
                    status_code=status,
                    content_type=ctype,
                    body_preview=body_preview,
                )

            # NOMADS often returns HTML/text errors instead of GRIB2
            if "text/html" in ctype or "text/plain" in ctype:
                try:
                    body_preview = r.text[:1500]
                except Exception:
                    body_preview = "<unable to read response body>"
                raise NomadsDownloadError(
                    "NOMADS returned non-GRIB payload",
                    url=url,
                    status_code=status,
                    content_type=ctype,
                    body_preview=body_preview,
                )

            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)

    except requests.exceptions.RequestException as e:
        raise NomadsDownloadError(
            f"NOMADS request failed: {type(e).__name__}: {e}",
            url=url,
        )

    tmp.replace(dest)

    if not dest.exists() or dest.stat().st_size == 0:
        raise NomadsDownloadError("Downloaded GRIB2 file was empty", url=url)

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

    # Derived winds if vectors are present
    u10 = result.get("u10")
    v10 = result.get("v10")
    if u10 is not None and v10 is not None:
        result["wind_speed_10m"] = math.hypot(u10, v10)  # m/s
        result["wind_dir_10m"] = compute_meteorological_dir_deg(u10, v10)
    else:
        result["wind_speed_10m"] = None
        result["wind_dir_10m"] = None

    u925 = result.get("u925")
    v925 = result.get("v925")
    if u925 is not None and v925 is not None:
        result["wind_speed_925"] = math.hypot(u925, v925)  # m/s
        result["wind_dir_925"] = compute_meteorological_dir_deg(u925, v925)
    else:
        result["wind_speed_925"] = None
        result["wind_dir_925"] = None

    return result


def init_hourly_dict() -> Dict[str, List[Optional[float]]]:
    return {k: [] for k in ALL_HOURLY_KEYS}


def merge_non_null(target: Dict[str, Optional[float]], src: Dict[str, Optional[float]]) -> None:
    for k, v in src.items():
        if v is not None:
            target[k] = v


def build_bundle_url(
    bundle_name: str,
    lat: float,
    lon: float,
    cycle_date: str,
    cycle_hour: int,
    fhr: int,
) -> str:
    cfg = BUNDLE_CONFIGS[bundle_name]
    return nomads_hrrr_url(
        lat,
        lon,
        cycle_date,
        cycle_hour,
        fhr,
        product=cfg["product"],
        flags=cfg["flags"],
    )


def cycle_candidate_dts(start_dt: datetime, max_back_cycles: int) -> List[datetime]:
    # HRRR runs hourly; walk back one hour each attempt
    return [start_dt - timedelta(hours=i) for i in range(max_back_cycles + 1)]


def _invalid_parameter_in_body(body: Optional[str]) -> bool:
    if not body:
        return False
    return "invalid parameter:" in body.lower()


def resolve_cycle_for_required_surface_core(
    lat: float,
    lon: float,
    fhrs: List[int],
    start_cycle_dt: datetime,
    *,
    force_refresh: bool = False,
    max_back_cycles: int = 12,
) -> Tuple[datetime, List[str]]:
    """
    Find the first cycle where surface_core files for all requested forecast hours exist.
    Uses cache, so later extraction reuses downloads.
    """
    warnings: List[str] = []
    last_errors: List[str] = []

    for candidate_dt in cycle_candidate_dts(start_cycle_dt, max_back_cycles=max_back_cycles):
        cycle_date, cycle_hour = cycle_dt_to_parts(candidate_dt)
        candidate_ok = True
        candidate_errors: List[str] = []

        for fhr in fhrs:
            url = build_bundle_url("surface_core", lat, lon, cycle_date, cycle_hour, fhr)
            try:
                download_with_cache(url, force_refresh=force_refresh)
            except NomadsDownloadError as e:
                # Fail fast if the request itself is invalid (config bug), not a cycle availability issue.
                if _invalid_parameter_in_body(e.body_preview):
                    raise HTTPException(
                        status_code=500,
                        detail={
                            "error": "Invalid NOMADS parameter in required surface_core bundle",
                            "bundle": "surface_core",
                            "cycle_tested": f"{cycle_date}{cycle_hour:02d}",
                            "fhr": fhr,
                            "nomads_url": e.url,
                            "status_code": e.status_code,
                            "content_type": e.content_type,
                            "body_preview": e.body_preview,
                        },
                    )

                candidate_ok = False
                msg = f"cycle {cycle_date}{cycle_hour:02d} f{fhr:02d} surface_core unavailable: {e.short()}"
                if e.body_preview:
                    snippet = re.sub(r"\s+", " ", e.body_preview)[:220]
                    msg += f" | body={snippet}"
                candidate_errors.append(msg)
                break  # no need to test more fhrs for this cycle

        if candidate_ok:
            if candidate_dt != start_cycle_dt:
                warnings.append(
                    f"Selected fallback HRRR cycle {cycle_date}{cycle_hour:02d} "
                    f"(initial candidate {start_cycle_dt.strftime('%Y%m%d%H')} unavailable)."
                )
            return candidate_dt, warnings

        last_errors.extend(candidate_errors[-2:])

    detail = {
        "error": "Unable to find a usable HRRR surface_core cycle for requested forecast hours",
        "start_cycle_utc": start_cycle_dt.strftime("%Y%m%d%H"),
        "max_back_cycles": max_back_cycles,
        "recent_errors": last_errors[-6:],
    }
    raise HTTPException(status_code=502, detail=detail)


def fetch_bundle_fields_for_hour(
    bundle_name: str,
    lat: float,
    lon: float,
    cycle_date: str,
    cycle_hour: int,
    fhr: int,
    *,
    force_refresh: bool = False,
) -> Tuple[Optional[Dict[str, Optional[float]]], str]:
    """
    Returns (fields_or_none, url). Raises NomadsDownloadError only on download issues.
    Extraction errors bubble as HTTPException.
    """
    url = build_bundle_url(bundle_name, lat, lon, cycle_date, cycle_hour, fhr)
    grib_file = download_with_cache(url, force_refresh=force_refresh)
    fields = extract_fields(grib_file, lat, lon)
    return fields, url


def start_cycle_dt_from_query(run_utc: Optional[str]) -> datetime:
    if run_utc:
        return parse_run_utc_to_dt(run_utc)
    return auto_start_cycle_dt()


def summarize_nomads_error_for_warning(bundle_name: str, fhr: int, e: NomadsDownloadError) -> str:
    msg = f"Bundle {bundle_name} f{fhr:02d} skipped: {e.short()}"
    if e.body_preview:
        body = re.sub(r"\s+", " ", e.body_preview)[:260]
        msg += f" | body={body}"
    return msg


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/debug/hrrr_inventory")
def debug_hrrr_inventory(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    fhr: int = Query(1, ge=0, le=48),
    run_utc: Optional[str] = Query(None, description="YYYYMMDDHH or ISO8601 Z"),
    bundle: str = Query(
        "surface_core",
        pattern="^(surface_core|surface_diag|pbl|clouds|thermo_stability|pressure_low)$",
    ),
    force_refresh: bool = Query(False),
    max_back_cycles: int = Query(12, ge=0, le=48),
):
    start_dt = start_cycle_dt_from_query(run_utc)

    selected_dt, fallback_warnings = resolve_cycle_for_required_surface_core(
        lat,
        lon,
        [fhr],
        start_dt,
        force_refresh=force_refresh,
        max_back_cycles=max_back_cycles,
    )

    cycle_date, cycle_hour = cycle_dt_to_parts(selected_dt)
    url = build_bundle_url(bundle, lat, lon, cycle_date, cycle_hour, fhr)

    try:
        grib_file = download_with_cache(url, force_refresh=force_refresh)
    except NomadsDownloadError as e:
        raise HTTPException(
            status_code=502,
            detail={
                "error": f"{bundle} bundle unavailable for selected cycle",
                "bundle": bundle,
                "selected_run_utc": f"{cycle_date}{cycle_hour:02d}",
                "nomads_url": e.url,
                "status_code": e.status_code,
                "content_type": e.content_type,
                "body_preview": e.body_preview,
                "warnings": fallback_warnings,
            },
        )

    inv = grib_inventory(grib_file)

    return JSONResponse(
        {
            "meta": {
                "source": "nomads",
                "model": "hrrr",
                "bundle": bundle,
                "product": BUNDLE_CONFIGS[bundle]["product"],
                "run_utc_requested": run_utc,
                "run_utc_selected": f"{cycle_date}{cycle_hour:02d}",
                "fhr": fhr,
                "nomads_url": url,
            },
            "warnings": fallback_warnings,
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
    max_back_cycles: int = Query(12, ge=0, le=48),
):
    try:
        fh_list = parse_fhrs(fhrs)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    start_dt = start_cycle_dt_from_query(run_utc)

    selected_dt, warnings = resolve_cycle_for_required_surface_core(
        lat,
        lon,
        fh_list,
        start_dt,
        force_refresh=force_refresh,
        max_back_cycles=max_back_cycles,
    )
    cycle_date, cycle_hour = cycle_dt_to_parts(selected_dt)
    run_dt = datetime.strptime(f"{cycle_date}{cycle_hour:02d}", "%Y%m%d%H").replace(tzinfo=timezone.utc)

    hourly = init_hourly_dict()

    debug_urls: Dict[str, List[str]] = {"surface_core": []}
    for b in OPTIONAL_BUNDLE_ORDER:
        debug_urls[b] = []

    optional_bundle_disabled_reason: Dict[str, str] = {}

    for idx, fhr in enumerate(fh_list):
        merged_fields: Dict[str, Optional[float]] = {
            # raw/direct
            "u10": None,
            "v10": None,
            "gust_10m": None,
            "mslp": None,
            "u925": None,
            "v925": None,
            "u850": None,
            "v850": None,
            "z850": None,
            "cape": None,
            "cin": None,
            "lifted_index": None,
            "pbl_height": None,
            "cloud_cover_total": None,
            "cloud_cover_low": None,
            "cloud_cover_mid": None,
            "cloud_cover_high": None,
            "temp_2m_k": None,
            "temp_850_k": None,
            # extracted derived from bundle-level extract_fields
            "wind_speed_10m": None,
            "wind_dir_10m": None,
            "wind_speed_925": None,
            "wind_dir_925": None,
        }

        # 1) required surface core
        try:
            fields, url = fetch_bundle_fields_for_hour(
                "surface_core", lat, lon, cycle_date, cycle_hour, fhr, force_refresh=force_refresh
            )
            debug_urls["surface_core"].append(url)
            if fields:
                merge_non_null(merged_fields, fields)
        except NomadsDownloadError as e:
            raise HTTPException(
                status_code=502,
                detail={
                    "error": "surface_core failed after cycle selection",
                    "run_utc_selected": f"{cycle_date}{cycle_hour:02d}",
                    "fhr": fhr,
                    "bundle": "surface_core",
                    "nomads_url": e.url,
                    "status_code": e.status_code,
                    "content_type": e.content_type,
                    "body_preview": e.body_preview,
                    "warnings": warnings,
                },
            )

        # 2) optional bundles (best-effort)
        for bundle_name in OPTIONAL_BUNDLE_ORDER:
            if bundle_name in optional_bundle_disabled_reason:
                if idx == 0:
                    warnings.append(
                        f"Bundle {bundle_name} disabled for remaining hours: "
                        f"{optional_bundle_disabled_reason[bundle_name]}"
                    )
                continue

            try:
                fields, url = fetch_bundle_fields_for_hour(
                    bundle_name, lat, lon, cycle_date, cycle_hour, fhr, force_refresh=force_refresh
                )
                debug_urls[bundle_name].append(url)
                if fields:
                    merge_non_null(merged_fields, fields)
            except NomadsDownloadError as e:
                warn = summarize_nomads_error_for_warning(bundle_name, fhr, e)
                warnings.append(warn)
                # If clearly invalid/unsupported, stop retrying this optional bundle for remaining hours
                if e.status_code in (400, 404, 500) or _invalid_parameter_in_body(e.body_preview):
                    optional_bundle_disabled_reason[bundle_name] = warn
            except HTTPException as e:
                warn = f"Bundle {bundle_name} f{fhr:02d} extraction skipped: {e.detail}"
                warnings.append(warn)
                optional_bundle_disabled_reason[bundle_name] = warn

        # Final derived winds from merged vectors (m/s)
        u10 = merged_fields.get("u10")
        v10 = merged_fields.get("v10")
        u925 = merged_fields.get("u925")
        v925 = merged_fields.get("v925")

        ws10_si = math.hypot(u10, v10) if (u10 is not None and v10 is not None) else None
        wd10 = compute_meteorological_dir_deg(u10, v10) if (u10 is not None and v10 is not None) else None

        ws925_si = math.hypot(u925, v925) if (u925 is not None and v925 is not None) else None
        wd925 = compute_meteorological_dir_deg(u925, v925) if (u925 is not None and v925 is not None) else None

        gust_si = merged_fields.get("gust_10m")

        # Regime-derived metrics
        surface_925_vec_diff_si = None
        if None not in (u10, v10, u925, v925):
            assert u10 is not None and v10 is not None and u925 is not None and v925 is not None
            surface_925_vec_diff_si = math.hypot(u925 - u10, v925 - v10)

        surface_925_dir_diff_deg = angle_diff_deg(wd10, wd925)

        # Proxy for 0–1km shear: use 10m vs 925mb vector diff (m/s)
        shear_0_1km_proxy_si = surface_925_vec_diff_si

        mixing_efficiency_ratio = None
        if ws10_si is not None and ws925_si is not None and ws925_si > 0:
            mixing_efficiency_ratio = ws10_si / ws925_si

        temp_2m_c = k_to_c(merged_fields.get("temp_2m_k"))
        temp_850_c = k_to_c(merged_fields.get("temp_850_k"))
        temp_diff_2m_850_c = None
        if temp_2m_c is not None and temp_850_c is not None:
            temp_diff_2m_850_c = temp_2m_c - temp_850_c

        valid_dt = run_dt + timedelta(hours=fhr)
        hourly["time"].append(valid_dt.isoformat().replace("+00:00", "Z"))

        # Raw vectors (keep SI)
        hourly["u10"].append(u10)
        hourly["v10"].append(v10)
        hourly["u925"].append(u925)
        hourly["v925"].append(v925)
        hourly["u850"].append(merged_fields.get("u850"))
        hourly["v850"].append(merged_fields.get("v850"))

        # Directions / non-speed scalars
        hourly["wind_dir_10m"].append(wd10)
        hourly["wind_dir_925"].append(wd925)
        hourly["mslp"].append(merged_fields.get("mslp"))
        hourly["z850"].append(merged_fields.get("z850"))
        hourly["cape"].append(merged_fields.get("cape"))
        hourly["cin"].append(merged_fields.get("cin"))
        hourly["lifted_index"].append(merged_fields.get("lifted_index"))
        hourly["pbl_height"].append(merged_fields.get("pbl_height"))

        hourly["cloud_cover_total"].append(merged_fields.get("cloud_cover_total"))
        hourly["cloud_cover_low"].append(merged_fields.get("cloud_cover_low"))
        hourly["cloud_cover_mid"].append(merged_fields.get("cloud_cover_mid"))
        hourly["cloud_cover_high"].append(merged_fields.get("cloud_cover_high"))

        hourly["temp_2m_c"].append(temp_2m_c)
        hourly["temp_850_c"].append(temp_850_c)
        hourly["temp_diff_2m_850_c"].append(temp_diff_2m_850_c)

        # Speed-like values (respect units query)
        if units == "knots":
            conv = 1.943844492
            hourly["wind_speed_10m"].append(ws10_si * conv if ws10_si is not None else None)
            hourly["gust_10m"].append(gust_si * conv if gust_si is not None else None)
            hourly["wind_speed_925"].append(ws925_si * conv if ws925_si is not None else None)
            hourly["surface_925_wind_diff"].append(surface_925_vec_diff_si * conv if surface_925_vec_diff_si is not None else None)
            hourly["shear_0_1km_proxy_10m_925"].append(shear_0_1km_proxy_si * conv if shear_0_1km_proxy_si is not None else None)
        else:
            hourly["wind_speed_10m"].append(ws10_si)
            hourly["gust_10m"].append(gust_si)
            hourly["wind_speed_925"].append(ws925_si)
            hourly["surface_925_wind_diff"].append(surface_925_vec_diff_si)
            hourly["shear_0_1km_proxy_10m_925"].append(shear_0_1km_proxy_si)

        hourly["surface_925_dir_diff_deg"].append(surface_925_dir_diff_deg)
        hourly["mixing_efficiency_ratio_10m_to_925"].append(mixing_efficiency_ratio)

    response = {
        "meta": {
            "source": "nomads",
            "model": "hrrr",
            "run_utc_requested": run_utc,
            "run_utc_selected": f"{cycle_date}{cycle_hour:02d}",
            "cycle_hour": cycle_hour,
            "units": units,
            "bundle_strategy": {k: {"product": v["product"], "required": v["required_for_point"]} for k, v in BUNDLE_CONFIGS.items()},
            "derived_features": [
                "surface_925_wind_diff (vector magnitude difference; same speed units as output)",
                "shear_0_1km_proxy_10m_925 (proxy using 10m vs 925mb vector difference)",
                "mixing_efficiency_ratio_10m_to_925 (10m speed / 925mb speed)",
                "temp_diff_2m_850_c (2mC - 850mbC)",
                "surface_925_dir_diff_deg (absolute direction difference)",
            ],
            "notes": [
                "GRIB2 decoded with wgrib2 nearest-gridpoint extraction.",
                "Wrapper uses multi-bundle best-effort fetches and merges fields.",
                "Optional bundles may be skipped if NOMADS rejects the request or product/fields are unavailable.",
                "Temperature outputs are converted from Kelvin to Celsius when available.",
                "0–1 km shear is currently a proxy using 10m vs 925mb wind vector difference.",
            ],
        },
        "location": {"lat": lat, "lon": lon},
        "warnings": warnings,
        "hourly": hourly,
        "debug": {
            "nomads_urls_by_bundle": debug_urls
        },
    }

    return response