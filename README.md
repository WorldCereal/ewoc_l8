# ESA WorldCereal - Landsat-8 processor

# Description 

This processor will:

* Download Landsat-8 products from the usgs landsat aws bucket
* Convert the downloaded products into EWoC ARD format
* Upload the processed files to a separate bucket in order to be used in the classification 
# Installation
1. First grab the latest versions of ewoc_dag and eotile from the github
2. `pip install .`

You can also pull the latest docker image from the harbour or ecr registry

# Usage

Docker

```bash
sudo docker run -ti --rm --env-file env.dev ewoc_l8 --verbose v l8_id -pid "LC08_L1TP_201035_20191022_20200825_02_T1 LC08_L1TP_201034_20191022_20200825_02_T1" -t 30STG -o ../out
```

Python on host 

```bash
python ewoc_l8 --verbose v l8_id -pid "LC08_L1TP_201035_20191022_20200825_02_T1 LC08_L1TP_201034_20191022_20200825_02_T1" -t 30STG -o ../out --sr"
```
