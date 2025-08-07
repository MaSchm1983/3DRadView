import h5py
import numpy as np
import os
import json
import matplotlib.pyplot as plt
import contextily as ctx
from pyproj import CRS, Transformer

# === USER SETTINGS ===
DATA_DIR = "3D_RAD_DATA"
TARGET_TIMESTAMP = "20250805092000"  # YYYYMMDDHHMMSS

# === COMPOSITE GRID PARAMETERS ===
COMPOSITE_N = 900
COMPOSITE_DX = 1000  # 1 km
COMPOSITE_PROJ = CRS(proj="stere", lat_0=90, lon_0=10, lat_ts=60,
                     x_0=0, y_0=0, a=6370040, b=6370040, units="m", datum="WGS84")
COMPOSITE_CENTER_LON = 9.0
COMPOSITE_CENTER_LAT = 51.0

composite_fwd = Transformer.from_crs("EPSG:4326", COMPOSITE_PROJ, always_xy=True)
cx, cy = composite_fwd.transform(COMPOSITE_CENTER_LON, COMPOSITE_CENTER_LAT)

# === 1. Get all files for this timestamp ===
with open(os.path.join(DATA_DIR, "current_files.json"), "r") as f:
    files_index = json.load(f)

selected_files = []
for entry in files_index:
    relpath = entry["path"]
    filename = os.path.basename(relpath)
    if TARGET_TIMESTAMP in filename:
        selected_files.append(os.path.join(DATA_DIR, relpath))

print(f"Found {len(selected_files)} radar files for {TARGET_TIMESTAMP}")

# === 2. Prepare composite grid ===
composite = np.full((COMPOSITE_N, COMPOSITE_N), np.nan)
radar_centers = []

# === 3. Loop over radar files and paste them into composite grid ===
for fname in selected_files:
    try:
        with h5py.File(fname, "r") as f:
            raw = f["dataset1/data1/data"][:]
            raw = np.flip(raw, axis=-2)  # 
            attrs_what = f["dataset1/data1/what"].attrs
            gain = attrs_what["gain"]
            offset = attrs_what["offset"]
            nodata = attrs_what["nodata"]
            undetect = attrs_what["undetect"]
            dbz = raw * gain + offset
            dbz[(raw == nodata) | (raw == undetect) | (raw == 0)] = np.nan
            dbz[dbz < 10] = np.nan  # Mask low reflectivity
            
            # Get radar grid size and center from /where group
            attrs_where = f["where"].attrs
            radar_center_lon = attrs_where.get("lon", np.nan)
            radar_center_lat = attrs_where.get("lat", np.nan)
            xsize = int(attrs_where.get("xsize", raw.shape[-1]))
            ysize = int(attrs_where.get("ysize", raw.shape[-2]))

            print(f"{os.path.basename(fname)}: center=({radar_center_lon},{radar_center_lat}), shape={dbz.shape}")

            if (xsize != 400) or (ysize != 400):
                print(f"⚠️ File {fname} has non-400x400 grid, skipping.")
                continue

            # Save radar center for plotting
            if not (np.isnan(radar_center_lon) or np.isnan(radar_center_lat)):
                radar_centers.append((radar_center_lon, radar_center_lat))

            # Project radar center to composite grid X/Y
            fwd = composite_fwd
            radar_x, radar_y = fwd.transform(radar_center_lon, radar_center_lat)
            ix_center = int(round((radar_x - cx) / COMPOSITE_DX + COMPOSITE_N // 2))
            iy_center = int(round((radar_y - cy) / COMPOSITE_DX + COMPOSITE_N // 2))

            half = 200  # 400/2
            ix0, ix1 = max(0, ix_center - half), min(COMPOSITE_N, ix_center + half)
            iy0, iy1 = max(0, iy_center - half), min(COMPOSITE_N, iy_center + half)
            rx0, rx1 = max(0, half - ix_center), 400 - max(0, ix_center + half - COMPOSITE_N)
            ry0, ry1 = max(0, half - iy_center), 400 - max(0, iy_center + half - COMPOSITE_N)

            if (ix1 > ix0) and (iy1 > iy0):
                dbz_max = np.nanmax(dbz, axis=0)
                sl_comp = composite[iy0:iy1, ix0:ix1]
                sl_radar = dbz_max[ry0:ry1, rx0:rx1]
                both = (~np.isnan(sl_comp)) & (~np.isnan(sl_radar))
                only_radar = (~np.isnan(sl_radar)) & (np.isnan(sl_comp))
                sl_comp[both] = np.maximum(sl_comp[both], sl_radar[both])
                sl_comp[only_radar] = sl_radar[only_radar]
                composite[iy0:iy1, ix0:ix1] = sl_comp
            else:
                print(f"⚠️ Skipping {fname}: indices out of bounds.")


    except Exception as e:
        print(f"❌ Error reading {fname}: {e}")

# === 4. Create lat/lon grid for plotting ===
ix = np.arange(COMPOSITE_N)
iy = np.arange(COMPOSITE_N)
X = cx + (ix - COMPOSITE_N // 2) * COMPOSITE_DX
Y = cy + (iy - COMPOSITE_N // 2) * COMPOSITE_DX
Xc, Yc = np.meshgrid(X, Y)
composite_back = Transformer.from_crs(COMPOSITE_PROJ, "EPSG:4326", always_xy=True)
composite_lon, composite_lat = composite_back.transform(Xc, Yc)

# === 5. Plot ===
fig, ax = plt.subplots(figsize=(13, 12))
alpha = np.where(np.isnan(composite), 0, 0.7)
mesh = ax.pcolormesh(composite_lon, composite_lat, composite, cmap="turbo",
                     shading="auto", vmin=20, vmax=50, alpha=alpha)
plt.colorbar(mesh, ax=ax, label="Reflectivity (dBZ)")
ax.set_title(f"DWD Composite Max Reflectivity {TARGET_TIMESTAMP} UTC\n>20 dBZ, radar centers as dots")

# Plot radar centers
for lon, lat in radar_centers:
    ax.plot(lon, lat, 'o', color='black', alpha=0.5, markersize=9, zorder=10)

ctx.add_basemap(ax, crs="EPSG:4326", source=ctx.providers.OpenStreetMap.Mapnik)
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.tight_layout()
plt.savefig(f"composite_osm_{TARGET_TIMESTAMP}_simple.png", dpi=180)
plt.show()