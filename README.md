# ESA WorldCereal - Landsat-8 processor

## Description

This processor will:

* Download Landsat-8 products from the usgs landsat aws bucket
* Merge and reproject the Landsat-8 data to the S2 grid
* Convert the reprojected products into EWoC ARD format
* Upload the processed files to a separate bucket in order to be used in the classification

## Installation

1. `python3 -m venv .venv`
2. `source .venv/bin/activate`
3. `pip install pip -U`
4. `pip install dataship-0.1.10.tar.gz`
5. `pip install .`

You can also pull the latest docker image from the harbour or ecr registry

## Usage

Docker

```bash
sudo docker run -ti --rm --env-file env.dev ewoc_l8 --verbose v l8_id -pid "LC08_L1TP_201035_20191022_20200825_02_T1 LC08_L1TP_201034_20191022_20200825_02_T1" -t 30STG -o ../out
```

Python on host

```bash
ewoc_l8 --verbose v l8_id -pid "LC08_L1TP_201035_20191022_20200825_02_T1 LC08_L1TP_201034_20191022_20200825_02_T1" -t 30STG -o ../out --sr
```
