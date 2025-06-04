import h5py
import numpy as np
import matplotlib.pyplot as plt
import os

def load_radar_data(filepath):
    with h5py.File(filepath, 'r') as f:
        # --- Load raw radar data
        raw = f["dataset1/data1/data"][:]
        print("Raw data shape:", raw.shape)  # Expecting (levels, x, y)

        # --- Attempt to extract radar location (if present)
        lat = f["where/lat"][()] if "lat" in f["where"] else None
        lon = f["where/lon"][()] if "lon" in f["where"] else None

    # --- Convert to float for plotting
    data = raw.astype(np.float32)

    # --- Mask common "nodata" values (0 and 65535 are common placeholders)
    data = np.ma.masked_where((raw == 0) | (raw == 65535), data)

    return data, lat, lon

def plot_radar_slice(data, lat=None, lon=None):
    # Choose center vertical level (if more than 1)
    level = data.shape[0] // 2
    slice2d = data[level]

    # Plot
    plt.figure(figsize=(8, 7))
    im = plt.imshow(
        slice2d,
        cmap="turbo",
        origin="lower",
        vmin=0,
        vmax=80
    )
    cbar = plt.colorbar(im, label="Reflectivity (dBZ)")
    plt.title(f"Radar Reflectivity (Level {level})" +
              (f"\nLat: {lat:.2f}, Lon: {lon:.2f}" if lat and lon else ""))
    plt.xlabel("X Grid Points")
    plt.ylabel("Y Grid Points")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    radar_file = "rab02-pz_10132-20250529110000-deboo-hd5"

    if not os.path.exists(radar_file):
        print("❌ File not found:", radar_file)
    else:
        data, lat, lon = load_radar_data(radar_file)
        plot_radar_slice(data, lat=lat, lon=lon)