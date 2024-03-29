# Landsat-8 EWoC ARD processor

## Description

This processor will:

* Download [Landsat-8 Collection 2 Level-2 Science products](https://www.usgs.gov/landsat-missions/landsat-collection-2-level-2-science-products) from the [usgs landsat aws bucket](https://registry.opendata.aws/usgs-landsat/)
* Merge and reproject the Landsat-8 data to the S2 grid
* Convert the reprojected products into EWoC ARD format
* Upload the processed files to a separate bucket in order to be used in the classification

## Installation

1. `python3 -m venv .venv`
2. `source .venv/bin/activate`
3. `pip install pip -U`
4. `pip install ewoc_dag-x.y.z.tar.gz` available [here](https://github.com/WorldCereal/ewoc_dataship/releases)
5. `pip install .`

You can also pull the latest docker image from the EWoC harbour.

## Usage

To download and upload data you need to configure some env variable from `ewoc_dag` as described [here](https://github.com/WorldCereal/ewoc_dataship#usage).

### CLI Python

Run help command with:

```bash
ewoc_generate_l8_ard --help
```

Compute EWoC full (SR and TIR) ARD for a couple of L8 C2 L2 products:

```bash
ewoc_generate_l8_ard 31TCJ LC08_L2SP_199029_20211216_20211223_02_T1 LC08_L2SP_199030_20211216_20211223_02_T1
```

Compute EWoC TIR ARD for a couple of L8 C2 L2 products:

```bash
ewoc_generate_l8_ard --only-tir 31TCJ LC08_L2SP_199029_20211216_20211223_02_T1 LC08_L2SP_199030_20211216_20211223_02_T1
```

Compute EWoC SR ARD for a couple of L8 C2 L2 products:

```bash
ewoc_generate_l8_ard --only-sr 31TCJ LC08_L2SP_199029_20211216_20211223_02_T1 LC08_L2SP_199030_20211216_20211223_02_T1
```

Compute EWoC SR ARD mask for a couple of L8 C2 L2 products:

```bash
ewoc_generate_l8_ard --only-sr-mask 31TCJ LC08_L2SP_199029_20211216_20211223_02_T1 LC08_L2SP_199030_20211216_20211223_02_T1
```
