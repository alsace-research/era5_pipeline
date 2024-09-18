import pandas as pd
import h3
import numpy as np
import tempfile
import os
from dask import delayed
import xarray as xr
import gcsfs  # Required to handle files from GCS
import h3.api.numpy_int as h3_numpy  # Vectorized H3 using numpy_int API
import shutil  # For cleaning up the temporary directory
from datetime import datetime

def list_gcs_files(gcs_path):
    """List all files that match a pattern in Google Cloud Storage."""
    fs = gcsfs.GCSFileSystem()
    if gcs_path.startswith("gs://"):
        file_list = fs.glob(gcs_path)
        if not file_list:
            raise ValueError(f"No files found for pattern {gcs_path}")
        return file_list
    else:
        raise ValueError(f"Path {gcs_path} is not a valid GCS path")

def process_time_slice_futures(lat, lon, precipitation_data, timestamp, resolution=4, threshold=0.0001):
    """Process a single time slice using a vectorized H3 operation and precipitation threshold."""
    lat_flat = np.repeat(lat, len(lon))
    lon_flat = np.tile(lon, len(lat))
    precipitation_flat = precipitation_data.flatten()

    # Apply the threshold to filter out very low precipitation values
    valid_indices = precipitation_flat >= threshold

    lat_valid = lat_flat[valid_indices]
    lon_valid = lon_flat[valid_indices]
    precipitation_valid = precipitation_flat[valid_indices] * 1000  # Convert from meters to millimeters

    # Vectorized H3 conversion using numpy's vectorize
    h3_converter = np.vectorize(h3_numpy.geo_to_h3)
    h3_indices = h3_converter(lat_valid, lon_valid, resolution)

    # Convert the timestamp to full datetime including hour
    timestamp_full = pd.Timestamp(timestamp).isoformat()

    # Create a DataFrame with H3 indices, precipitation, and timestamp (including hour)
    hex_data = pd.DataFrame({
        'h3_index': h3_indices,
        'precipitation': precipitation_valid,
        'timestamp': timestamp_full  # This now includes both date and hour
    })

    return hex_data


def load_and_process_day_of_files(file_pattern, client, chunk_size, output_dir, threshold=0.0001, resolution=8):
    """Load and process a day of hourly files from GCS by downloading them to temporary files."""
    temp_dir = tempfile.mkdtemp()  # Create a temporary directory to store downloaded files
    try:
        fs = gcsfs.GCSFileSystem()
        file_list = list_gcs_files(file_pattern)
        
        if not file_list:
            raise ValueError(f"No files found for pattern {file_pattern}")

        datasets = []

        # Download each file to a temporary directory and then process it
        for file in file_list:
            temp_file_path = os.path.join(temp_dir, os.path.basename(file))
            print(f"Downloading {file} to {temp_file_path}")
            fs.get(file, temp_file_path)
            
            # Open the dataset from the temporary file using the netCDF4 engine
            ds = xr.open_dataset(temp_file_path, engine='netcdf4', chunks=chunk_size)
            datasets.append(ds)

        # Combine all datasets along the time dimension
        ds_combined = xr.concat(datasets, dim='time')

        # Extract latitude, longitude, and time data
        lat = ds_combined['latitude'].values
        lon = ds_combined['longitude'].values
        time_data = ds_combined['time'].values

        # Prepare Dask tasks for each time slice
        futures = []
        for t_idx, timestamp in enumerate(time_data):
            # Load the precipitation data chunk for this time slice
            precipitation_data = ds_combined['tp'].isel(time=t_idx).load()

            # Submit task for processing the time slice
            future = delayed(process_time_slice_futures)(
                lat,
                lon,
                precipitation_data.values,
                timestamp,
                resolution=resolution,
                threshold=threshold
            )
            futures.append(future)

        # Compute Dask futures in parallel
        results = client.compute(futures)
        final_df = pd.concat(client.gather(results))

        # Get the date from the first timestamp in the processed data
        date_str = pd.to_datetime(time_data[0]).strftime('%Y-%m-%d')

        # Output file path based on date
        output_file = os.path.join(output_dir, f"precipitation_{date_str}.parquet")
        
        # Save DataFrame to parquet
        final_df.to_parquet(output_file, compression='snappy')
        print(f"Saved data to {output_file}")

        return final_df

    except Exception as e:
        print(f"Error processing files matching pattern {file_pattern}: {e}")
        return pd.DataFrame()

    finally:
        # Cleanup the temporary directory after processing is done
        shutil.rmtree(temp_dir)
        print(f"Cleaned up temporary directory: {temp_dir}")


def convert_timestamps_to_pandas(df):
    """Convert cftime.DatetimeGregorian to pandas.Timestamp."""
    if 'timestamp' in df.columns:
        df['timestamp'] = df['timestamp'].apply(lambda x: pd.Timestamp(x.isoformat()))
    return df

def save_to_parquet(df, output_path):
    """Save the DataFrame to Parquet, partitioned by week, with h3_index and timestamp as the index."""
    # Convert timestamps to pandas and add a 'week' column
    df = convert_timestamps_to_pandas(df)
    df['week'] = df['timestamp'].dt.isocalendar().week

    # Set index with h3_index and timestamp
    df.set_index(['h3_index', 'timestamp'], inplace=True)

    # Sort the data for better read performance
    df.sort_values(['h3_index', 'timestamp'], inplace=True)

    # Ensure the output path is treated as a directory
    output_dir = output_path if output_path.endswith('/') else output_path + '/'

    # Save the DataFrame to a Parquet file partitioned by 'week'
    df.to_parquet(output_dir, partition_cols=['week'], compression='snappy')
    print(f"Saved DataFrame to {output_dir}")

def filter_by_date_range(df, start_date, end_date):
    """Filter the DataFrame to include only data within the specified date range."""
    if 'timestamp' not in df.columns:
        raise KeyError("'timestamp' column not found in DataFrame.")
    return df[(df['timestamp'] >= pd.Timestamp(start_date)) & (df['timestamp'] <= pd.Timestamp(end_date))]

def process_and_save_by_period(df, output_path, start_date, period_days=4):
    """Process the DataFrame and save in intervals of `period_days`."""
    # Convert timestamps to pandas
    df = convert_timestamps_to_pandas(df)

    # Calculate the period based on `period_days`
    df['period'] = ((df['timestamp'] - pd.Timestamp(start_date)).dt.days // period_days) + 1

    # Set the index with h3_index and timestamp
    df.set_index(['h3_index', 'timestamp'], inplace=True)

    # Sort the data for better read performance
    df.sort_values(['h3_index', 'timestamp'], inplace=True)

    # Save the DataFrame partitioned by 'period'
    df.to_parquet(output_path, partition_cols=['period'], compression='snappy')
    print(f"Saved DataFrame to {output_path}")
