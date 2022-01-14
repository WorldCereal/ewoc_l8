import json
import logging
import subprocess
from pathlib import Path
from typing import List, Dict

from eotile.eotile_module import main
import numpy as np
import rasterio

logger = logging.getLogger(__name__)


def json_to_dict(path_to_json: str):
    with open(path_to_json, encoding="utf8") as f:
        data = json.load(f)
    return data

def ard_from_key(
    key: str,
    s2_tile: str,
    band_num: str,
    out_dir: Path = None
):
    sr_bands = ["B2", "B3", "B4", "B5", "B6", "B7", "QA_PIXEL_SR"]
    st_bands = ["B10", "QA_PIXEL_TIR"]
    if band_num in sr_bands:
        measure_type = "OPTICAL"
    elif band_num in st_bands:
        measure_type = "TIR"
    else:
        logging.error("Unknown band")
    product_id = Path(key).parts[-1]
    platform = product_id.split("_")[0]
    processing_level = product_id.split("_")[1]
    processing_level_folder = "L1T"
    date = product_id.split("_")[3]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = s2_tile
    unique_id = f"{product_id.split('_')[2]}{product_id.split('_')[5]}{product_id.split('_')[6]}"
    folder_st = Path(measure_type) / tile_id[:2] / tile_id[2] / tile_id[3:] / year / date.split("T")[0]
    dir_name = (
        f"{platform}_{processing_level_folder}_{date}T235959_{unique_id}_{tile_id}"
    )
    out_name = f"{platform}_{processing_level}_{date}T235959_{unique_id}_{tile_id}"
    raster_fn = folder_st / dir_name / out_name
    if out_dir is not None:
        tmp = Path(out_dir) / folder_st / dir_name
        tmp.mkdir(parents=True, exist_ok=False)
    return raster_fn


def get_tile_info(s2_tile_id: str):
    s2_tile = main(s2_tile_id)[0]
    s2_tile_srs = (s2_tile["SRS"].values)[0]
    logger.info("SRS of %s is %s", s2_tile_id, s2_tile_srs)

    s2_tile_UL0 = list(s2_tile["UL0"])[0]
    s2_tile_UL1 = list(s2_tile["UL1"])[0]
    s2_tile_bb = (s2_tile_UL0, s2_tile_UL1 - 109800, s2_tile_UL0 + 109800, s2_tile_UL1)
    logger.info("Bounding box of %s is %s", s2_tile_id, s2_tile_bb)

    return s2_tile_srs, s2_tile_bb

def key_from_id(pid: str):
    info = pid.split("_")
    date1 = info[3]
    date2 = info[4]
    year = info[3][:4]
    path, row = info[2][:3], info[2][3:]
    key = f"s3://usgs-landsat/collection02/level-2/standard/oli-tirs/{year}/{path}/{row}/LC08_L2SP_{path}{row}_{date1}_{date2}_02_T1/LC08_L2SP_{path}{row}_{date1}_{date2}_02_T1_ST_B10.TIF"
    return key


def get_mask(sr_qa_pix: Path):
    with rasterio.open(sr_qa_pix, "r") as src:
        meta = src.meta.copy()
        meta["dtype"] = "uint8"
        meta["nodata"] = 255
        qa_pixel_array = src.read(1)

        # Define the nodata
        nodata = qa_pixel_array == 1

        # Define the to-be-masked qa_pixel_array values based on the bitmask
        cirrus = 1 << 2
        cloud = 1 << 3
        shadow = 1 << 4
        snow = 1 << 5

        # Construct the "clear" mask
        clear = (
            (qa_pixel_array & shadow == 0)
            & (qa_pixel_array & cloud == 0)
            & (qa_pixel_array & cirrus == 0)
            & (qa_pixel_array & snow == 0)
        )

        # Contruct the final binary 0-1-255 mask
        cld_mask = np.zeros_like(qa_pixel_array)
        cld_mask[nodata] = 255
        cld_mask[clear] = 1
        cld_mask[nodata] = 255

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

    logging.info("Binary cloud mask - Done")


def rescale_array(array: List[int], factors: Dict[int, int]):
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
    array = array * factors["a"] + factors["b"]
    array[array < 0] = 0
    return array.astype(np.uint16)


def raster_to_ard(raster_path: Path, band_num: str, raster_fn: Path, factors: Dict[float, float] = None):
    """
    Read raster and update internals to fit ewoc ard specs
    :param raster_path: Path to raster file
    :param band_num: Band number, B02 for example
    :param raster_fn: Output raster path
    :param factors: dictionary of factors for a rescale of the raster values
    """
    bands_sr = ["B2", "B3", "B4", "B5", "B6", "B7"]
    with rasterio.Env(GDAL_CACHEMAX=2048):
        with rasterio.open(raster_path, "r") as src:
            raster_array = src.read()
            meta = src.meta.copy()
            if band_num in bands_sr:
                raster_array = rescale_array(raster_array, factors)
    meta["driver"] = "GTiff"
    if band_num != "QA_PIXEL_TIR":
        meta["nodata"] = 0
    if band_num == "QA_PIXEL_SR":
        meta["nodata"] = 255
    bands_10m = ["B2", "B3", "B4", "B5"]
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

def execute_cmd(cmd: str):
    """
    Execute the given cmd.
    :param cmd: The command and its parameters to execute
    """
    logger.debug("Launching command: %s", cmd)
    try:
        subprocess.run(cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            check=True)
    except subprocess.CalledProcessError as err:
        logger.error(f'Following error code {err.returncode} \
            occurred while running command {err.cmd} with following output:\
            {err.stdout} / {err.stderr}')
