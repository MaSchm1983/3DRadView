import h5py
import numpy as np
import matplotlib.pyplot as plt
import os
from pyproj import CRS, Transformer

# === PARAMETERS ===
# Radar center (decimal degrees)
lat0 = 47.8736
lon0 = 8.0036

# Grid spacing in meters
dx = dy = 1000  # 1 km

# Projection: Polar Stereographic
crs_stereo = CRS(proj='stere',
                 lat_0=lat0,
                 lon_0=lon0,
                 lat_ts=60,
                 datum='WGS84')

# Transformer to convert from projection to lat/lon
transformer = Transformer.from_crs(crs_stereo, CRS("EPSG:4326"), always_xy=True)

# === FUNCTIONS ===

def decode_reflectivity(raw_data):
    data = raw_data.astype(np.float32)
    data = (data / 2.0) - 32.5
    # Mask out invalid or noisy values
    data = np.ma.masked_where((data < 0) | (data > 70), data)
    return data

def generate_latlon_grid_edges(width, height, dx=1000, dy=1000):
    # Create center-based XY grid
    x = (np.arange(width) - width / 2 + 0.5) * dx
    y = (np.arange(height) - height / 2 + 0.5) * dy

    # Now compute cell edges
    x_edges = (np.arange(width + 1) - width / 2) * dx
    y_edges = (np.arange(height + 1) - height / 2) * dy

    X_edges, Y_edges = np.meshgrid(x_edges, y_edges)

    # Project to lat/lon
    lon_edges, lat_edges = transformer.transform(X_edges, Y_edges)
    return lat_edges, lon_edges

def plot_and_save_all_levels(data, lat_grid, lon_grid, out_dir="output_dbz"):
    os.makedirs(out_dir, exist_ok=True)
    n_levels = data.shape[0]

    for level in range(n_levels):
        slice2d = data[level]

        plt.figure(figsize=(9, 8))
        im = plt.pcolormesh(
            lon_grid,
            lat_grid,
            slice2d,
            cmap="turbo",
            shading='auto',
            vmin=-32.5,
            vmax=40
        )
        plt.colorbar(im, label="Reflectivity (dBZ)")
        plt.title(f"Decoded Reflectivity – Level {level}")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.tight_layout()

        filename = os.path.join(out_dir, f"reflectivity_level_{level:02d}.png")
        plt.savefig(filename, dpi=150)
        plt.close()
        print(f"✅ Saved: {filename}")

def main():
    radar_file = "rab02-pz_10908-20250604175000-defbg-hd5"  # <- Replace this with your actual file

    if not os.path.exists(radar_file):
        print("❌ File not found:", radar_file)
        return

    with h5py.File(radar_file, 'r') as f:
        raw = f["dataset1/data1/data"][:]
        print("📦 Raw shape:", raw.shape)

    # Decode reflectivity from raw values
    dbz = decode_reflectivity(raw)

    # Create lat/lon grid
    lat_grid, lon_grid = generate_latlon_grid_edges(width=raw.shape[2], height=raw.shape[1])

    # Plot and save
    plot_and_save_all_levels(dbz, lat_grid, lon_grid)

if __name__ == "__main__":
    main()
