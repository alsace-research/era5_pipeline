import pandas as pd
import h3
import numpy as np
import tempfile
import netCDF4 as nc
from dask.distributed import Client

import h3.api.numpy_int as h3_numpy  # Use the numpy_int API for vectorized operations

import h3.api.basic_str as h3_numpy  # Change to basic_str to handle scalar inputs
import numpy as np

def process_time_slice_futures(lat, lon, precipitation_data, timestamp, resolution=8, threshold=0.0001):
    """Process a single time slice using a vectorized H3 operation and precipitation threshold."""
    lat_flat = np.repeat(lat, len(lon))
    lon_flat = np.tile(lon, len(lat))
    precipitation_flat = precipitation_data.flatten()

    # Apply the threshold to filter out very low precipitation values
    valid_indices = precipitation_flat >= threshold

    lat_valid = lat_flat[valid_indices]
    lon_valid = lon_flat[valid_indices]
    precipitation_valid = precipitation_flat[valid_indices]

    # Vectorized H3 conversion using numpy's vectorize
    h3_converter = np.vectorize(h3_numpy.geo_to_h3)
    h3_indices = h3_converter(lat_valid, lon_valid, resolution)

    # Ensure timestamp retains the full datetime (including hours and minutes)
    hex_data = pd.DataFrame({
        'h3_index': h3_indices,
        'precipitation': precipitation_valid,
        'timestamp': timestamp  # Ensure this is a full datetime, not just the date
    })

    return hex_data




import pandas as pd

def convert_timestamps_to_pandas(df):
    """Convert cftime.DatetimeGregorian to pandas.Timestamp."""
    if 'timestamp' in df.columns:
        # Convert cftime.DatetimeGregorian to pandas.Timestamp
        df['timestamp'] = df['timestamp'].apply(lambda x: pd.Timestamp(x.isoformat()))
    return df



# def load_and_process_file(file_path, fs, client, threshold=0.0001, resolution=8):
#     """Download and process the precipitation data for a specific file from GCS."""
#     try:
#         with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
#             print(f"Downloading file from: {file_path}")
#             fs.get(file_path, tmp_file.name)
#             ds = nc.Dataset(tmp_file.name)

#             # Get latitude, longitude, and time data
#             lat = ds.variables['latitude'][:]
#             lon = ds.variables['longitude'][:]
#             time_var = ds.variables['time']
#             time_units = time_var.units
#             time_data = nc.num2date(time_var[:], units=time_units)

#             # Process each time slice using futures
#             futures = []
#             for t_idx, timestamp in enumerate(time_data):
#                 precipitation_data = ds.variables['tp'][t_idx, :, :]
#                 future = client.submit(process_time_slice_futures, lat, lon, precipitation_data, timestamp, resolution=resolution, threshold=threshold)
#                 futures.append(future)

#             # Gather results
#             results = client.gather(futures)
#             final_df = pd.concat(results)
#             return final_df

#     except Exception as e:
#         print(f"Error processing file {file_path}: {e}")
#         return pd.DataFrame()

import xarray as xr
import gcsfs
import pandas as pd
from dask.distributed import Client
import tempfile

def load_and_process_file(file_path, fs, client, threshold=0.0001, resolution=8):
    """Download and process the precipitation data for a specific file from GCS with chunking."""
    try:
        with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
            print(f"Downloading file from: {file_path}")
            fs.get(file_path, tmp_file.name)

            # Open the file with Xarray and apply chunking
            ds = xr.open_dataset(tmp_file.name, chunks={'time': 10, 'latitude': 700, 'longitude': 1700})

            # Extract latitude, longitude, and time data
            lat = ds['latitude'].values
            lon = ds['longitude'].values
            time_data = ds['time'].values

            # Prepare Dask tasks for each time slice
            futures = []
            for t_idx, timestamp in enumerate(time_data):
                # Load the precipitation data chunk for this time slice
                precipitation_data = ds['tp'].isel(time=t_idx).load()

                # Submit task for processing the time slice
                future = client.submit(
                    process_time_slice_futures,
                    lat,
                    lon,
                    precipitation_data.values,  # Convert to NumPy array for processing
                    timestamp,
                    resolution=resolution,
                    threshold=threshold
                )
                futures.append(future)

            # Gather results
            results = client.gather(futures)
            final_df = pd.concat(results)
            return final_df

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return pd.DataFrame()


def save_to_parquet(df, output_path):
    """Save the DataFrame to Parquet, optimized for querying by h3_index and timestamp."""
    # Convert timestamps and set index
    df = convert_timestamps_to_pandas(df)
    df.set_index(['h3_index', 'timestamp'], inplace=True)

    # Sort the data for better read performance
    df.sort_values(['h3_index', 'timestamp'], inplace=True)

    # Save the DataFrame to a single Parquet file with compression
    df.to_parquet(output_path, compression='snappy')
    print(f"Saved DataFrame to {output_path}")
