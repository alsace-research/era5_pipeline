import os
import gcsfs
import xarray as xr

import warnings

# Suppress warnings about Compute Engine Metadata server being unavailable
warnings.filterwarnings('ignore', message='Compute Engine Metadata server unavailable')


def download_era5_data(year, month, day, hour, config):
    fs = gcsfs.GCSFileSystem()  # Create a GCS file system object
    base_url = f"{config['data']['storage_path']}{year}/{month:02d}/{day:02d}/total_precipitation/surface.nc"
    local_path = f"./data/raw/{year}/{month:02d}/{day:02d}/surface_{hour:02d}.nc"
    
    if not os.path.exists(os.path.dirname(local_path)):
        os.makedirs(os.path.dirname(local_path))

    if not os.path.exists(local_path):
        fs.get(base_url, local_path)  # Download the file from GCS
    return local_path
