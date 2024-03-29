from datetime import date
import logging
from pathlib import Path
import shutil
from tempfile import gettempdir
from typing import List, Optional, Tuple

import boto3
from ewoc_dag.bucket.ewoc import EWOCARDBucket
from ewoc_dag.l8c2l2_dag import get_l8c2l2_gdal_path
from ewoc_dag.eo_prd_id.l8_prd_id import L8C2PrdIdInfo

from ewoc_l8 import EWOC_L8_INPUT_DOWNLOAD_ERROR
from ewoc_l8.utils import (ard_from_key, get_mask, key_from_id,
                           raster_to_ard, get_tile_info, execute_cmd)

logger = logging.getLogger(__name__)

class L8ARDProcessorBaseError(Exception):
    """ Base Error"""
    def __init__(self, exit_code, l8_prd_ids):
        self._message = "Error during L8 ARD generation:"
        self.exit_code = exit_code
        self._l8_prd_ids = l8_prd_ids
        super().__init__(self._message)

class L8InputProcessorError(L8ARDProcessorBaseError):
    """Exception raised for errors in the L8 ARD generation at input download step."""
    def __init__(self, prd_ids):
        super().__init__(EWOC_L8_INPUT_DOWNLOAD_ERROR, prd_ids)

    def __str__(self):
        return f"{self._message} {self._l8_prd_ids} not download !"

def generate_l8_band_ard(
    band_num: str,
    tr_group: List[str],
    production_id: str,
    t_srs: str,
    s2_tile: str,
    bnds:Tuple[float, float, float, float],
    res: str,
    out_dir: Path,
    no_upload: bool = False,
    debug: bool = False
)->Tuple[int,int,str,str]:
    """
    Process Landsat-8 band: Download, merge and clip to S2 tile footprint
    For one band, one date
    :param band_num: Landsat-8 band name, accepted values:
        ['B2','B3','B4','B5','B6','B7','B10','QA','QA_PIXEL']
    :param tr_group: A list of s3 ids for Landsat-8 raster on the usgs-landsat bucket
    :param production_id: Production ID that will be used to upload to s3 bucket
    :param t_srs: Target projection system, to determined from the Sentinel-2 tile projection
    :param s2_tile: The id of the targeted Sentinel-2 ex 31TCJ (Toulouse)
    :param bnds: Extent of the Sentinel-2 tile, you can get this using
         the function get_bounds from dataship/ewoc_dag
    :param res: Resampling resolution, could be 10 or 20 meters
    :param out_dir: Output directory to store the temporary results,
         should be deleted on full completion
    :param no_upload: If True the ard files are not uploaded to s3 bucket
    :param debug: If True all the intermediate files and results will be kept locally
    :return: Number of uploaded objects (0 or 1), size of uploaded object, upload path and bucket name 
    """
    # Create list of same bands but different dates
    l8_to_s2 = {
        "B2": "B02",
        "B3": "B03",
        "B4": "B04",
        "B5": "B08",
        "B6": "B11",
        "B7": "B12",
        "B10": "B10",
        "QA_PIXEL_SR": "MASK",
        "QA_PIXEL_TIR": "QA_PIXEL",
    }

    if band_num in ["QA_PIXEL_TIR", "QA_PIXEL_SR"]:
        sr_method = "near"
        dst_nodata = "-dstnodata 1"
    else:
        sr_method = "bilinear"
        dst_nodata = ""
    band_num_alias = l8_to_s2[band_num]

    group_bands = []
    ewoc_ard_bucket = EWOCARDBucket()
    for prd_id in tr_group:
        prd_date, __unused = get_band_key(band_num, prd_id)
        group_bands.append(prd_id)

    tmp_folder = out_dir / 'tmp' / str(prd_date) / str(band_num)
    src_folder = out_dir / 'tmp'
    tmp_folder.mkdir(parents=True, exist_ok=False)
    group_bands = list(set(group_bands))
    group_bands.sort()
    ref_name = key_from_id(group_bands[0])

    vsi_gdal_paths=[]
    for band in group_bands:
        raster_folder = tmp_folder / band

        if band_num in ['B2', 'B3','B4','B5','B6','B7',]:
            prd_item = 'SR_' + band_num
        elif band_num == 'B10':
            prd_item = 'ST_B10'
        else:
            prd_item = 'QA_PIXEL'

        vsi_gdal_paths.append(get_l8c2l2_gdal_path(band, prd_item))

    logger.info(vsi_gdal_paths)
    config_options = ["--config AWS_REQUEST_PAYER requester",
                      "--config AWS_REGION us-west-2",
                      "--config CPL_VSIL_CURL_ALLOWED_EXTENSIONS .tif",
                      "--config GDAL_DISABLE_READDIR_ON_OPEN YES"]

    logger.info("Checking needed L8 bands in the product prefix")
    for vsi_gdal_path in vsi_gdal_paths:
        s3_result = boto3.client("s3").list_objects_v2(
        Bucket='usgs-landsat',
        Prefix=vsi_gdal_path[20:],
        Delimiter = "/",
        RequestPayer="requester"
        )
        if s3_result.get('Contents') is None:
            logger.error("Band %s does not exist !", vsi_gdal_path.split('/')[-1])
            if not debug:
                shutil.rmtree(src_folder)
            raise L8InputProcessorError(tr_group)

    try:
        logger.info("Starting Re-projection")
        raster_folder.mkdir()
        raster_list=[]
        for vsi_gdal_path in vsi_gdal_paths:
            r_raster = (raster_folder/vsi_gdal_path.split("/")[-1]).with_suffix('.vrt')
            logger.info(r_raster)
            cmd_proj = f"gdalwarp -of VRT {' '.join(config_options)} -tr {res} {res} -r {sr_method} -t_srs {t_srs} {vsi_gdal_path} {r_raster} {dst_nodata}"
            logger.info(cmd_proj)
            execute_cmd(cmd_proj)
            raster_list.append(str(r_raster))

        logger.info("Starting VRT creation")
        merge_raster_filepath = raster_folder/'merge_l8.vrt'
        cmd_vrt = f"gdalbuildvrt {' '.join(config_options)} -q {merge_raster_filepath} {' '.join(raster_list)}"
        execute_cmd(cmd_vrt)

        logger.info("Starting Clip to S2 extent")
        tiled_raster_filepath = raster_folder/'tiled_l8.tif'
        cmd_clip = f"gdalwarp {' '.join(config_options)} -te {bnds[0]} {bnds[1]} {bnds[2]} {bnds[3]} {merge_raster_filepath} {tiled_raster_filepath}"
        execute_cmd(cmd_clip)

        upload_name = (
            str(ard_from_key(ref_name, band_num=band_num, s2_tile=s2_tile))
            + f"_{band_num_alias}.tif"
        )
        upload_path = "/".join([production_id, upload_name])

        logger.info("Converting to EWoC ARD")
        ewoc_ard_filepath = raster_folder / (s2_tile + '_ard.tif')
        if "QA_PIXEL" in band_num and "SR" in band_num:
            raster_to_ard(
                get_mask(tiled_raster_filepath),
                band_num,
                ewoc_ard_filepath,
                prd_date,
            	tr_group,
            )
        else:
            raster_to_ard(
                tiled_raster_filepath,
                band_num,
                ewoc_ard_filepath,
                prd_date,
            	tr_group,
            )

        if not no_upload:
            up_file_size= ewoc_ard_bucket.upload_ard_raster(
                ewoc_ard_filepath, upload_path
            )

        return 1, up_file_size, upload_path, ewoc_ard_bucket.bucket_name
    except BaseException as err:
        logger.info("Failed for group\n")
        logger.info(tr_group)
        logger.info(err)
        return 0, 0, "", ""
    finally:
        if not debug:
            shutil.rmtree(src_folder)

def generate_l8_ard(
    tr_group: List[str],
    production_id: str,
    s2_tile: str,
    out_dir: Path,
    only_sr: bool = False,
    only_sr_mask: bool = False,
    only_tir: bool = False,
    no_upload: bool = False,
    debug: bool = False
)->Tuple[int,List[str]]:
    """
    Process a group of Landsat-8 ids, full bands or thermal only
    :param tr_group: A list of s3 ids for Landsat-8 raster on the usgs-landsat bucket
    :param s2_tile: The id of the targeted Sentinel-2 ex 31TCJ (Toulouse)
    :param production_id: Production ID that will be used to upload to s3 bucket
    :type production_id: str
    :param out_dir: Output directory to store the temporary results,
         should be deleted on full completion
    :param only_sr: Process only SR bands, default to False
    :param only_sr_mask: Process only SR masks, default to False
    :param only_tir: Process only TIR bands, default to False
    :param no_upload: If True the ard files are not uploaded to s3 bucket, default to False
    :param debug: If True all the intermediate files and results will be kept locally,
         default to False
    :return: Number of uploaded objects and list of s3 paths
    """
    res_dict = {
        "B2": "10",
        "B3": "10",
        "B4": "10",
        "B5": "10",
        "B6": "20",
        "B7": "20",
        "B10": "30",
        "QA_PIXEL_SR": "20",
        "QA_PIXEL_TIR": "30",
    }

    if only_sr and only_sr_mask and only_tir:
        raise ValueError("Request to process sr only and tir only and sr mask only!")
    if only_tir and only_sr:
        raise ValueError("Request to process sr only and tir only!")
    if only_sr and only_sr_mask:
        raise ValueError("Request to process sr only and sr mask only!")
    if only_tir and only_sr_mask:
        raise ValueError("Request to process tir only and sr mask only!")

    if only_sr_mask:
        process_bands = ["QA_PIXEL_SR"]
    elif only_sr:
        process_bands = ["B2", "B3", "B4", "B5", "B6", "B7", "QA_PIXEL_SR"]
    elif only_tir:
        process_bands = ["B10", "QA_PIXEL_TIR"]
    else:
        process_bands = ["QA_PIXEL_TIR", "B2", "B3", "B4", "B5", "B6", "B7", "B10", "QA_PIXEL_SR"]
    logger.info("Following bands will be processed: %s", process_bands)

    t_srs, bnds = get_tile_info(s2_tile)

    upload_count = 0
    total_size = 0
    paths = []

    for band in process_bands:
        logger.info("Processing %s band", band)
        up_count, up_size, upload_path, bucket_name = generate_l8_band_ard(
            band,
            tr_group,
            production_id,
            t_srs,
            s2_tile,
            bnds,
            res=res_dict[band],
            out_dir=out_dir,
            no_upload=no_upload,
            debug=debug,
        )
        upload_count += up_count
        total_size += up_size
        if str(Path(upload_path).parent) not in paths:
            paths.append(str(Path(upload_path).parent))
    optic_path = None
    tir_path = None
    for path in paths:
        if "TIR" in path:
            tir_path = path
        if "OPTIC" in path:
            optic_path = path

    path_list = []
    if tir_path is not None:
        path_list.append(f"s3://{bucket_name}/{tir_path}")
    if optic_path is not None:
        path_list.append(f"s3://{bucket_name}/{optic_path}")

    if not no_upload:
        logging_string = f"Uploaded {upload_count} tif files to bucket |" + (
        " ; ".join(path_list)
        )
        print(logging_string)

    return upload_count, path_list

def get_band_key(band: str, prd_id: str)->Tuple[date,Optional[str]]:
    """
    Get the S3 band id from band name
    :param band: Band number B2/B3/B4/B5/B6/B7/B10/QA
    :param tr: Thermal band s3 id
    :return: The date of the product and the s3 key
    """
    sr_bands = ["B2", "B3", "B4", "B5", "B6", "B7"]
    qa_bands = ["QA_PIXEL_SR", "QA_PIXEL_TIR"]
    st_bands = ["B10"]

    key = None
    if band in st_bands:
        key = f"{prd_id}_ST_{band.upper()}.TIF"
    elif band in sr_bands:
        key = f"{prd_id}_SR_{band.upper()}.TIF"
    elif band in qa_bands:
        key = f"{prd_id}_QA_PIXEL.TIF"
    else:
        ValueError("Band ID {band} not valid!")

    return L8C2PrdIdInfo(prd_id).acquisition_date, key

if __name__ == "__main__":

    _S2_TILE_ID = "30SVG"

    upload_count, path_list = generate_l8_ard(
        ["LC08_L1TP_201035_20191022_20200825_02_T1",
        "LC08_L1TP_201034_20191022_20200825_02_T1"],
        _S2_TILE_ID,
        "0000_000_0000",
        Path(gettempdir()),
        only_sr=True,
        only_sr_mask=False,
        only_tir=False,
        no_upload=False,
        debug=True,
    )
