import pytest
import pandas as pd
from unittest.mock import patch
from src.pipeline import run_pipeline

@pytest.fixture
def config():
    """Return a test configuration."""
    return {
        'data': {
            'output_path': './test_output',
            'storage_path': 'gs://test_bucket'
        },
        'processing': {
            'start_date': '2022-01-01',
            'end_date': '2022-01-04',
            'period_days': 4
        },
        'dask': {
            'num_workers': 2,
            'memory_limit': '4GB',
            'local_directory': './test_dask_space'
        }
    }

@patch('src.pipeline.list_files_for_date_range')
@patch('src.pipeline.load_and_process_file')
@patch('src.pipeline.Client')
def test_run_pipeline(mock_dask_client, mock_load_and_process_file, mock_list_files, config):
    """Test the full pipeline flow with mock objects."""
    # Mock file listing and processing results
    mock_list_files.return_value = ['gs://test_bucket/file1.nc', 'gs://test_bucket/file2.nc']
    mock_load_and_process_file.return_value = pd.DataFrame({
        'h3_index': [1, 2],
        'timestamp': pd.to_datetime(['2022-01-01', '2022-01-02']),
        'precipitation': [0.1, 0.2]
    })
    
    # Run the pipeline
    run_pipeline(config)
    
    # Assert that the files were listed and processed
    mock_list_files.assert_called_once()
    mock_load_and_process_file.assert_called()
