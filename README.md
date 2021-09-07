# ESA WorldCereal - Landsat-8 processor

# Description 

This processor will:

* Download Landsat-8 products from the usgs landsat aws bucket
* Convert the downloaded products into EWoC ARD format
* Upload the processed files to a separate bucket in order to be used in the classification 
# Installation
1. First grab the latest versions of dataship (ewoc_dag) and eotile from the github
2. `pip install .`

You can also pull the latest docker image from the harbour or ecr registry

# Usage

Docker

```bash
sudo docker run -ti --rm --env-file env.dev ewoc_l8 --verbose v l8_id -pid "s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2019/222/067/LC08_L2SP_222067_20190315_20200829_02_T1/LC08_L2SP_222067_20190315_20200829_02_T1_ST_B10.TIF s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2019/222/066/LC08_L2SP_222066_20190315_20200829_02_T1/LC08_L2SP_222066_20190315_20200829_02_T1_ST_B10.TIF" 
-t 22LHQ -o ../../test_d_l8/test_debug --sr
```

Python on host 

```bash
python ewoc_l8 l8_id -pid "s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2019/222/067/LC08_L2SP_222067_20190315_20200829_02_T1/LC08_L2SP_222067_20190315_20200829_02_T1_ST_B10.TIF s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2019/222/066/LC08_L2SP_222066_20190315_20200829_02_T1/LC08_L2SP_222066_20190315_20200829_02_T1_ST_B10.TIF -t 22LHQ -o ../../test_d_l8/test_debug --sr"
```
