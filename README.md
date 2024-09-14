<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a id="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->



<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->


<!-- PROJECT LOGO -->
<br />
<div align="center">
  <h3 align="center">ERA5 Data Pipeline</h3>

  <p align="center">
    An method to turn ERA5 netCDF4 into Uber H3 hexagons!
    <br />
    <a href="https://github.com/othneildrew/Best-README-Template"><strong>Explore the docs »</strong></a>
    <br />
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
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#workflow">Workflow</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project


Here you'll find an common Earth Observation (EO) industry workflow to pull Satellite derived atmospheric data from netCDF4 format to Uber H3 format in parquet.  

The data comes from [Google Analysis-Ready & Cloud Optimized (ARCO) ERA5](https://console.cloud.google.com/storage/browser/gcp-public-data-arco-era5/raw/date-variable-single_level/2022/01/01/total_precipitation;tab=objects?pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))&prefix=&forceOnObjectsSortingFiltering=false)


representing hourly precipitation data for the globe for ERA5 from the the *European Centre for Medium-Range Weather Forecasts (ECMWF)*.

Here's why:
* **H3** offers an convenient spatial index to better combine data of multiple spatial formats into a single format.
* **H3** provides an simple way to store geospatial data without the storage overhead.


I appreciate you taking the time to look over this repo!


<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- GETTING STARTED -->
## Getting Started

First, we'll need to create an environment that allows us to run EO data using Python.


### Installation

_Below is an example of how you can instruct your audience on installing and setting up your app. This template doesn't rely on any external dependencies or services._

1. Clone the repo
   ```sh
   git clone https://github.com/alsace_research/era5_pipeline.git
   ```
2. Install `environment.yml` packages
   ```sh
   conda env create -f environment.yml        
   ```

4. Change git remote url to avoid accidental pushes to base project
   ```sh
   git remote set-url origin alsace-research/era5_pipeline
   git remote -v # confirm the changes
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- USAGE EXAMPLES -->
## The Data

1. The `total_precipitation` data comes in NetCDF4 format, which is not an optimized or compressed file format.  
2. The data comes at a global scale and a resolution of 80km x 80km pixel grids
3. The H3 resolution is of user choice, but I chose to use reolution 8, which 


<!-- USAGE EXAMPLES -->
## The Tooling

There are a few approaches to processing this type of data.  I started with the building the process locally on an M1 Mac.  For this approach, I chose to use `python`, `numpy`, `Dask`, `Xarray`, `gcsfs`, and `H3`.  

Some other approaches I considered, due to familiarity:
1. Databricks `Spark`: `spark-xarray`, `Mosaic` (native Databricks library for large-scale H3 processing - I've met the developers, the tools have been tested). [Link to Databricks Mosaic.](https://github.com/databrickslabs/mosaic)
2. Python: `Dask Distributed`, `Xarray`
3. Alternatives: `Coiled` (managed Dask clusters) via Cloud


**Local**

The Dask and Xarray stack has many similarities between a local cluster, distributed cluster, or a HPC cluster.  The difference lies in the configuration of the workflow.

**Distributed in Cloud**
The process will run Python code which can be distributed and scaled across many memory-optimized machines in the cloud.

The code will run the same in the cloud, just ensure the configuration is set for the cloud.

**HPC** High Perforamance Computing centers are often used for EO data and must be used appropriately.  The configuration shows an attempt an building an scheduler for this approach.

<!-- USAGE EXAMPLES -->
## Configuration

I've provided an configuration file for this workflow.  I was unsure what the expectation was for the assessment, so I attempted to provide an cofiguration that gives the user the ability to select their own compute setting: Local, Cloud, or HPC.

<!-- USAGE EXAMPLES -->
## Workflow

One can access the Dask dashboard at it's fixed location here: `http://127.0.0.1:8787/status`

1. Read in netCDF4 files from Google Cloud Storage
2. Load files in parallel using Dask
3. Chunk the data in Xarray to use 840MB to optimize memory in the read
4. Validate the Task Graph is not too large (this is where the chunk size is important)
5. Select the Data Range needed, ultimately we want to process a full year `2022` of precipitation data
6. Convert the netCDF4 into Uber H3 Hexagons in parallel
7. Aggregate each grid pixel to an single H3 index and take the mean value 
8. End with the h3_index, timestamp (hourly), precip_value (in mm), and resolution.
9. Export to parquet


<p align="right">(<a href="#readme-top">back to top</a>)</p>


_For more examples, please refer to the [Documentation](https://example.com)_

<p align="right">(<a href="#readme-top">back to top</a>)</p>





<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>
