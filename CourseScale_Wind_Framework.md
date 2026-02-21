# Course-Scale Wind Framework
## Quantitative Regime + Stability Model

---

# SECTION 1 — Derived Variables

ΔT = AirTemp − WaterTemp

DirectionalSpread = max(station wind dirs) − min(station wind dirs)

SpeedSpread = max(station wind speeds) − min(station wind speeds)

CoherenceIndex:
  High if DirectionalSpread < 15°
  Moderate if 15–30°
  Low if >30°

---

# SECTION 2 — Mixing Depth Proxy

Initialize MixingDepthScore = 0

+30 if wind speed > 15 kt
+20 if gust spread > 5 kt
+20 if sky cover < 50%
+15 if ΔT > 3°C during daylight hours
+15 if pressure steady (no rapid change)

Shallow penalties:
−30 if wind < 8 kt
−20 if overcast > 80%
−20 if ΔT < 0°C

Classification:

MixingDepthScore > 60 → Deep Mixed
30–60 → Moderate Mixing
<30 → Shallow / Stable

---

# SECTION 3 — Regime Scoring (0–100)

Initialize all to 0.

---

## 3.1 GradientScore

+40 if wind > 15 kt
+20 if CoherenceIndex High
+15 if DirectionalSpread < 10°
+15 if minimal diurnal signature
+10 if pressure gradient evident

Max 100

---

## 3.2 ThermalScore

+30 if ΔT > 3°C
+20 if time between 10–17 local
+20 if sky cover < 60%
+20 if gradient opposing component < 10 kt
+10 if temperature rising

Max 100

---

## 3.3 StableScore

+30 if ΔT < 0°C
+20 if wind < 8 kt
+20 if overcast/fog
+20 if CoherenceIndex Low
+10 if shallow MixingDepthScore

Max 100

---

## 3.4 TerrainScore

+30 if wind aligned through channel/gap
+20 if known venue acceleration zone
+20 if repeatable geographic shifts
+10 if shoreline curvature enhances backing/veering

Max 100

---

# SECTION 4 — Normalize Regime Mix

Total = Gradient + Thermal + Stable + Terrain

Each Regime% = RegimeScore / Total

---

# SECTION 5 — Oscillation Period Detection

From 3–6 hour time series:

1. Remove directional noise < 3°
2. Identify local maxima and minima
3. Measure time between consecutive peaks
4. If ≥2 cycles:
     Period = mean time between peaks
5. If 1 partial cycle:
     Period = estimated range (5–20 min if thermal)
6. If monotonic:
     Period = None

OscillationConfidence increases if:
- Similar pattern across 2+ stations
- Period consistent across stations

---

# SECTION 6 — Shift Mapping

If Gradient% > 60%:
  Type = Persistent
  Amplitude < 10°
  Period = None

If Thermal% > 50%:
  Type = Oscillating
  BaseAmplitude = 8–20°
  Period = 5–20 min
  Increase amplitude if MixingDepthScore 30–60
  Reduce amplitude if Deep Mixed

If Stable% > 50%:
  Type = Wandering
  Amplitude 10–30°
  Period Irregular

If Terrain% > 40%:
  Overlay geographic bias

Mixed regimes:
  Blend amplitude ranges proportionally.

---

# SECTION 7 — Confidence Model

DataScore:
  High coherence = +30
  Moderate = +15
  Low = 0

RegimeDominanceScore:
  One regime >60% = +30
  Mixed (40–60%) = +15
  Flat distribution = 0

MixingClarityScore:
  Deep or clear Shallow = +20
  Moderate mixing = +10
  Unclear = 0

TotalConfidenceScore = sum (max 80)

Confidence:
  60–80 = High
  35–59 = Medium
  <35 = Low
