import pytest
import pandas as pd
import numpy as np
from src.h3_processing import process_time_slice_futures, convert_timestamps_to_pandas

def test_process_time_slice_futures():
    """Test H3 conversion and precipitation filtering."""
    lat = np.array([37.7749, 37.7750])
    lon = np.array([-122.4194, -122.4195])
    precipitation_data = np.array([[0.1, 0.2], [0.0, 0.15]])  # 0.0 should be filtered out
    timestamp = pd.Timestamp('2022-01-01 00:00:00')
    
    result_df = process_time_slice_futures(lat, lon, precipitation_data, timestamp, resolution=8, threshold=0.01)

    # Check that only 3 points are retained after filtering
    assert len(result_df) == 3
    assert 'h3_index' in result_df.columns
    assert 'precipitation' in result_df.columns
    assert 'timestamp' in result_df.columns

def test_convert_timestamps_to_pandas():
    """Test timestamp conversion."""
    df = pd.DataFrame({'timestamp': [pd.Timestamp('2022-01-01')]})
    converted_df = convert_timestamps_to_pandas(df)
    
    assert pd.api.types.is_datetime64_any_dtype(converted_df['timestamp'])
