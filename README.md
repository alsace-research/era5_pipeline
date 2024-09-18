<a id="readme-top"></a>

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <h3 align="center">ERA5 Data Pipeline</h3>

  <p align="center">
    An method to turn ERA5 netCDF4 into Uber H3 hexagons!
    <br />
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
      </ul>
    </li>
    <li>
      <a href="#the-replication">The Replication</a>
      <ul>
        <li><a href="#installation">Installation</a></li>
        <li><a href="#the-configuration">The Configuration</a></li>
      </ul>
    </li>
    <li><a href="#the-data">The Data</a></li>  
    <li><a href="#the-approach">The Approach</a></li>  
    <li><a href="#the-workflow">The Workflow</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project


Here you'll find an common Earth Observation (EO) industry workflow to pull satellite derived atmospheric data from netCDF4 format to Uber H3 format in parquet.  

Here's the utility of H3:
* **H3** offers an convenient spatial index to better combine data of multiple spatial formats into a single format.
* **H3** provides an simple way to store geospatial data without the storage overhead.
* **H3** stores an int hash (h3_index) instead of expensive geometries (Point, Line, Polygons).  It is quite effificient at storing geospatial data.  

I appreciate your attention and getting an opportunity to share my thought process for tackling the above process.  


<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- GETTING STARTED -->
## The Replication

First, we'll need to create an environment that allows us to run EO data using Python.


### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/alsace_research/era5_pipeline.git
   ```
2. Install `environment.yml` packages
   ```sh
   conda env create -f environment.yml        
   ```

  This will create a conda environment called 'xarray'.  This library stack often has many interdependencies, so say a thanks to the conda-forge community to handling this overhead.  I chose `conda` because it is the best long term way to maintain these libraries and ensuring continued operations.



3. Go to the config.yaml file in the config folder
   ```sh
    data:
      storage_path: "gs://gcp-public-data-arco-era5/raw"
      file_pattern: "date-variable-single_level/{year}/{month:02d}/{day:02d}/total_precipitation/*.nc"
      output_dir: "./data/processed/"  # Directory to save the parquet files named by date
      threshold: 0.0001  # Precipitation threshold (in meters, to be converted to mm)
      resolution: 4  # H3 resolution for hexagons


    dask:
      scheduler: "distributed"
      num_workers: 4
      memory_limit: "4GB"  # Memory limit for Dask workers
      local_directory: "/tmp/dask-worker-space"
      dashboard_port: 8787  # Fixed port for Dask dashboard

    processing:
      start_year: 2022
      end_year: 2022
      start_date: "2022-01-01"
      end_date: "2022-01-07"  # Default set to one day for testing
      period_days: 1  # Number of days to process in each run (can be set to a larger number if needed)
      chunk_size:  # Specify chunk sizes for Dask processing
        latitude: 200  # Chunk size for latitude
        longitude: 200  # Chunk size for longitude
   ```


  I have this set to run a single week, but it will run an entire year.  Given that this was developed on an local environment, it will process files in batches ultimately producing parquet partitioned by a day.

4. Once the config is set to the setting of your choice, run the pipeline
  ```sh
  python run_pipeline.py       
  ```

5. If you'd like to test.  Ensure the `xarray` environment is active
  ```sh
  pytest  
  ```


### The Configuration


I've provided an configuration file for this workflow.  I was unsure what the expectation was for the assessment, so I attempted to provide an cofiguration that gives the user the ability to select their own compute setting: Local or Cloud.  


<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- USAGE EXAMPLES -->
## The Data

1. The `total_precipitation` data comes in NetCDF4 format, which is not an optimized or compressed file format.  The precipitation comes in meters.


2. The data comes from [Google Analysis-Ready & Cloud Optimized (ARCO) ERA5](https://console.cloud.google.com/storage/browser/gcp-public-data-arco-era5/raw/date-variable-single_level/2022/01/01/total_precipitation;tab=objects?pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))&prefix=&forceOnObjectsSortingFiltering=false) representing hourly precipitation data for the globe for ERA5 from the the *European Centre for Medium-Range Weather Forecasts (ECMWF)*.


3. Each precip grid pixel is ~28km x 28km (772 km2), we want to choose an H3 resolution to match.  H3 resolution `4` has an area of 1,770 km2.  i.e. ~2 precip grids fall inside a single H3.

4. The most optimized way to read this data is to leverage Lazy Loading in `Dask-distributed`.  This workflow instead reads in *chunks* of files at a time, processes them, and writes the output.

5. For the year of 2022, there are files for each hour, and each are 52MB in size (give or take).  There are 23 files for each day, ~8,600 for a year.  For loading in a distributed way, we tend to aim for loading chunks to 100MB to 1GB to keep memory efficient.  

6. One day of H3 hexagons amounts to ~3,636,140 records and ~64.5MB.  One year is estimated at 1,327,194,385 or ~1.3B records at the hourly time step.


<!-- USAGE EXAMPLES -->
## The Approach

There are a few approaches to processing this type of data.  I started with the building the process locally on an M1 Mac.  For this approach, I chose to use `python`, `numpy`, `Dask-distributed`, `xarray`, `gcsfs`, and `H3`.  

**I chose this approach because the code for a local workflow to a Cloud-optimized solution does not have a lot of variance aside from configuring the Dask client**.  The assessment was written quite broad, so I did my best to build an local and cloud optimized version.

Some other approaches I considered, due to familiarity:
1. Databricks `Spark`: `spark-xarray`, `Mosaic` (native Databricks library for large-scale H3 processing). [Link to Databricks Mosaic.](https://github.com/databrickslabs/mosaic)
2. Alternatives: `Coiled` (managed Dask clusters) + whatever flavor Cloud vendor


**Local**

The Dask and Xarray stack has many similarities between a local cluster, distributed cluster, or a HPC cluster.  The difference lies in the configuration of the workflow.  Though, it is fast enough to run locally.

**Distributed in Cloud**
The process will run Python code which can be distributed and scaled across many memory-optimized (ARM is a good choice) machines in the cloud.

The code will run the same in the cloud, just ensure the configuration is set for the cloud.

**HPC** High Performance Computing centers are often used for EO data and must be used appropriately.  The configuration shows an attempt an building an scheduler for this approach.  I evaluated added this to the config but opted out due to time concerns.



<!-- USAGE EXAMPLES -->
## The Workflow

One can access the Dask dashboard at it's fixed location here: `http://127.0.0.1:8787/status`

1. Read in netCDF4 files from Google Cloud Storage
2. Load files in parallel using Dask
3. Chunk the data in Xarray to ***{lat: 200, long: 200}***

4. Validate the Task Graph is not too large (this is where the chunk size is important) *I did not get time to build and visualize this*.
5. Select the Data Range needed, ultimately we want to process a full year `2022` of precipitation data, but feel free to run on a day or week to test.
6. Convert the netCDF4 into Uber H3 Hexagons in parallel
    - Each grid pixel has a cooresponding latitude, longitude, value
    - Convert precipitation from `meters` to `milimeters`
    - Sort by latitude, longitude before we process (spatial optimization)
    - Vectorize `np.vectorize(h3_numpy.geo_to_h3)`
7. Aggregate each grid pixel to an single H3 index and take the mean value 
    - Each grid pixel is ~28km x 28km (772 km2), we want to choose an H3 resolution to match.  
    - H3 resolution `4` has an area of 1,770 km2.  
    - i.e. ~2 precip grids fall inside a single H3.
    - Set index on `h3_index`, `timestamp`.
8. End with the h3_index, timestamp (hourly), precip_value (in mm).
9. Export to parquet w/ h3_index, timestamp as index
10. The parquet are optimized to be read by spatial query or by a time-series query.


<p align="right">(<a href="#readme-top">back to top</a>)</p>


## The Remaining Bits

While time is finite, there are always things left undone.  Here includes a list of items I'd hope to better evolve.

1. Better optimization from GCS to read into Dask & Xarray (lazy loading at scale).
1. Flesh out more robust testing - simple tests were chosen.
2. Full adaption of configuration for Cloud usage / HPC
3. General clean-up of docs used to communicate to the masses (or a sall team)


## The Appreciation

Again, a thank you for your time and attention.  A simple thank you for asking an relevant task for the assessment.  

It's a much more comfortable experience and much more efficient use of everyone's time.  It's enabled a better medium for sharing my experience and approach.

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>
