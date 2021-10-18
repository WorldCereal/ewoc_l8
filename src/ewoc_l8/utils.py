import json
import logging
import os

import boto3
import numpy as np
import rasterio

from eotile.eotile_module import main

logger = logging.getLogger(__name__)

def json_to_dict(path_to_json):
    with open(path_to_json) as f:
        data = json.load(f)
    return data

def make_dir(fold_dir):
    if not os.path.exists(fold_dir):
        os.makedirs(fold_dir)

def ard_from_key(key,s2_tile,band_num,out_dir=None):
    sr_bands = ['B2','B3','B4','B5','B6','B7','QA_PIXEL_SR']
    st_bands = ['B10','QA_PIXEL_TIR']
    if band_num in sr_bands:
        measure_type = "OPTICAL"
    elif band_num in st_bands:
        measure_type = "TIR"
    else:
        logging.error("Unknown band")
    product_id = os.path.split(key)[-1]
    platform = product_id.split('_')[0]
    processing_level = product_id.split('_')[1]
    processing_level_folder = "L1T"
    date = product_id.split('_')[3]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = s2_tile
    unique_id = f"{product_id.split('_')[2]}{product_id.split('_')[5]}{product_id.split('_')[6]}"
    folder_st = os.path.join(measure_type, tile_id[:2], tile_id[2], tile_id[3:], year,date.split('T')[0])
    dir_name = f"{platform}_{processing_level_folder}_{date}T235959_{unique_id}_{tile_id}"
    out_name = f"{platform}_{processing_level}_{date}T235959_{unique_id}_{tile_id}"
    raster_fn = os.path.join(folder_st, dir_name, out_name)
    if out_dir is not None:
        tmp = os.path.join(out_dir, folder_st, dir_name)
        if not os.path.exists(tmp):
            os.makedirs(tmp)
    return raster_fn

def binary_sr_qa(sr_qa_file):
    src = rasterio.open(sr_qa_file, "r")
    meta = src.meta.copy()
    ds = src.read(1)
    clear_px = [2, 4, 32, 66, 68, 96, 100, 130, 132, 160, 164]
    msk = np.isin(ds,clear_px).astype(int)
    ds[msk==1]=1
    ds[msk==0]=0
    raster_fn = sr_qa_file[:-4]+'_test.tif'
    with rasterio.open(
            raster_fn,
            "w+",
            **meta,
            compress="deflate",
            tiled=True,
            blockxsize=512,
            blockysize=512,
    ) as out:
        out.write(ds.astype(rasterio.uint16), 1)
    src.close()
    logging.info("Binary cloud mask - Done")


def download_s3file(s3_full_key,out_file, bucket):
    """
    Download file from s3 object storage
    :param s3_full_key: Object full path (bucket name, prefix, and key)
    :param out_file: Full path and name of the output file
    :param bucket: Bucket name
    """
    key = s3_full_key.split(bucket+"/")[1]
    s3 = boto3.resource("s3")
    object = s3.Object(bucket,key)
    resp = object.get(RequestPayer="requester")
    with open(out_file, "wb") as f:
        for chunk in iter(lambda: resp["Body"].read(4096), b""):
            f.write(chunk)

def get_tile_info(s2_tile_id:str):
    s2_tile = main(s2_tile_id)[0]
    s2_tile_srs = (s2_tile['SRS'].values)[0]
    logger.info('SRS of %s is %s', s2_tile_id, s2_tile_srs)

    s2_tile_UL0 = list(s2_tile["UL0"])[0]
    s2_tile_UL1 = list(s2_tile["UL1"])[0]
    s2_tile_bb = (s2_tile_UL0, s2_tile_UL1 - 109800, s2_tile_UL0 + 109800, s2_tile_UL1)
    logger.info('Bounding box of %s is %s',s2_tile_id, s2_tile_bb)

    return s2_tile_srs, s2_tile_bb

def key_from_id(pid):
    info = pid.split('_')
    date1 = info[3]
    date2 = info[4]
    year = info[3][:4]
    path, row = info[2][:3],info[2][3:]
    key = f"s3://usgs-landsat/collection02/level-2/standard/oli-tirs/{year}/{path}/{row}/LC08_L2SP_{path}{row}_{date1}_{date2}_02_T1/LC08_L2SP_{path}{row}_{date1}_{date2}_02_T1_ST_B10.TIF"
    return key

def get_mask(sr_qa_pix):
    src = rasterio.open(sr_qa_pix, "r")
    meta = src.meta.copy()
    meta['dtype']="uint8"
    del meta['nodata']
    data = src.read(1)
    cloud = 1<<3
    #full_mask = np.zeros_like(data)
    cld_mask = np.bitwise_and(data,cloud)
    cld_mask = cld_mask > 0
    raster_fn = sr_qa_pix
    with rasterio.open(
            raster_fn,
            "w+",
            **meta,
            compress="deflate",
            tiled=True,
            blockxsize=512,
            blockysize=512,
    ) as out:
        out.write(cld_mask.astype(rasterio.uint8), 1)
    src.close()
    logging.info("Binary cloud mask - Done")


def rescale_array(array, factors):
    """
    Rescales an array and forces it to np.uint16 :
    Applies array * factors['a'] + factors['b']
    :param array: The input array
    :param factors: A dictionary containing the integer factors
    :return:
    """
    if factors is None:
        logger.error("factors are undefined")
        raise ValueError
    logger.info("Rescaling Raster values")
    array = array * factors['a'] + factors['b']
    array[array < 0] = 0
    return array.astype(np.uint16)


def raster_to_ard(raster_path, band_num, raster_fn, factors=None):
    """
    Read raster and update internals to fit ewoc ard specs
    :param raster_path: Path to raster file
    :param band_num: Band number, B02 for example
    :param raster_fn: Output raster path
    :param factors: dictionary of factors for a rescale of the raster values
    """
    bands_sr = ['B2','B3','B4','B5','B6','B7']
    with rasterio.Env(GDAL_CACHEMAX=2048):
        with rasterio.open(raster_path,'r') as src:
            raster_array = src.read()
            if band_num in bands_sr:
                raster_array = rescale_array(raster_array, factors)
            meta = src.meta.copy()
    meta["driver"] = "GTiff"
    if band_num != "QA_PIXEL_TIR":
        meta["nodata"] = 0
    bands_10m = ['B2','B3','B4','B5']
    blocksize = 512
    if band_num in bands_10m:
        blocksize = 1024
    with rasterio.open(
        raster_fn,
        "w+",
        **meta,
        tiled=True,
        compress="deflate",
        blockxsize=blocksize,
        blockysize=blocksize,
    ) as out:
        out.write(raster_array)
