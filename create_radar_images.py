import os
import json
import h5py
import numpy as np
import matplotlib.pyplot as plt

# Paths (adjust as needed)
DATA_DIR = '3D_RAD_DATA'
OUTPUT_DIR = 'Images'
CURRENT_FILES_JSON = os.path.join(DATA_DIR, 'current_files.json')

# Filter site substring (or exact match)
FILTER_SITE = 'boo'

def load_current_files():
    with open(CURRENT_FILES_JSON, 'r') as f:
        return json.load(f)

def create_heatmap_image(data, threshold, out_path):
    masked_data = np.ma.masked_less(data, threshold)
    plt.figure(figsize=(8,6), dpi=100)
    plt.imshow(masked_data, origin='lower', cmap='jet', alpha=0.7)
    plt.axis('on')
    plt.savefig(out_path, bbox_inches='tight', pad_inches=0)
    plt.close()

def process_file(file_info):
    file_path = os.path.join(DATA_DIR, file_info['path'])
    timestamp = file_info['timestamp']

    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        return

    with h5py.File(file_path, 'r') as f:
        # Adjust 'reflectivity' to actual dataset name inside your hd5 file
        dataset = f['dataset1/data1/data'][:] 
        levels = dataset.shape[0]
        threshold = 20

        for lvl in range(levels):
            data = dataset[lvl, :, :]
            data = np.nan_to_num(data, nan=0)

            # Prepare output dir and filename
            site_dir = os.path.join(OUTPUT_DIR, file_info['site'])
            os.makedirs(site_dir, exist_ok=True)

            out_filename = f"{timestamp}_level{lvl}.png"
            out_path = os.path.join(site_dir, out_filename)

            create_heatmap_image(data, threshold, out_path)
            print(f"Saved image: {out_path}")

def main():
    files = load_current_files()
    filtered_files = [f for f in files if FILTER_SITE in f['site']]

    print(f"Processing {len(filtered_files)} files for site containing '{FILTER_SITE}'...")

    for file_info in filtered_files:
        process_file(file_info)

if __name__ == '__main__':
    main()