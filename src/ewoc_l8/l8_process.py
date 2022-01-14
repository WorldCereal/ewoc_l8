import logging
import os
from pathlib import Path
import shutil
from tempfile import gettempdir

from ewoc_dag.bucket.aws import AWSS2L8C2Bucket
from ewoc_dag.bucket.ewoc import EWOCARDBucket
from ewoc_l8.utils import (ard_from_key, get_mask, key_from_id,
                           raster_to_ard, get_tile_info, execute_cmd)

logger = logging.getLogger(__name__)


def process_group_band(
    band_num, tr_group, production_id, t_srs, s2_tile, bnds, res, out_dir, no_upload, debug
):
    """
    Process Landsat-8 band: Download, merge and clip to S2 tile footprint
    For one band, one date
    :param band_num: Landsat-8 band name, accepted values: ['B2','B3','B4','B5','B6','B7','B10','QA','QA_PIXEL']
    :param tr_group: A list of s3 ids for Landsat-8 raster on the usgs-landsat bucket
    :param t_srs: Target projection system, to determined from the Sentinel-2 tile projection
    :param s2_tile: The id of the targeted Sentinel-2 ex 31TCJ (Toulouse)
    :param bnds: Extent of the Sentinel-2 tile, you can get this using the function get_bounds from dataship/ewoc_dag
    :param res: Resampling resolution, could be 10 or 20 meters
    :param out_dir: Output directory to store the temporary results, should be deleted on full completion
    :param no_upload: If True the ard files are not uploaded to s3 bucket
    :param debug: If True all the intermediate files and results will be kept locally
    :return: Nothing
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

    s2_scaling_factor = 10000
    factors = {
        "a": 0.0000275 * s2_scaling_factor,
        "b": -0.2 * s2_scaling_factor,
    }  # Scaling factors

    if band_num in ["QA_PIXEL_TIR", "QA_PIXEL_SR"]:
        sr_method = "near"
        dst_nodata = "-dstnodata 1"
    else:
        sr_method = "bilinear"
        dst_nodata = ""
    band_num_alias = l8_to_s2[band_num]

    group_bands = []
    ewoc_ard_bucket = EWOCARDBucket()
    for tr in tr_group:
        # tr = key_from_id(tr)
        date, key = get_band_key(band_num, tr)
        group_bands.append(tr)

    tmp_folder = out_dir / 'tmp' / str(date) / str(band_num)
    src_folder = out_dir / 'tmp'
    tmp_folder.mkdir(parents=True, exist_ok=False)
    group_bands = list(set(group_bands))
    group_bands.sort()
    ref_name = key_from_id(group_bands[0])

    for band in group_bands:
        raster_folder = tmp_folder / band
        qa_bands = ["QA_PIXEL_SR", "QA_PIXEL_TIR"]
        if band_num in qa_bands:
            key = "QA_PIXEL"
        else:
            key = band_num

        AWSS2L8C2Bucket().download_prd(band, tmp_folder, prd_items=[key])
    try:
        logger.info("Starting Re-projection")
        for raster in os.listdir(raster_folder):
            raster = raster_folder / raster
            if res is not None:
                cmd_proj = f"gdalwarp -tr {res} {res} -r {sr_method} -t_srs {t_srs} {raster} {raster.with_suffix('')}_r.tif {dst_nodata}"
            else:
                cmd_proj = (
                    f"gdalwarp -t_srs {t_srs} {raster} {raster.with_suffix('')}_r.tif {dst_nodata}"
                )
        execute_cmd(cmd_proj)
        raster_list = " ".join(
            [
                str(raster_folder / rst)
                for rst in os.listdir(raster_folder)
                if rst.endswith("_r.tif")
            ]
        )

        logger.info("Starting VRT creation")
        cmd_vrt = f"gdalbuildvrt -q {raster_folder}/hrmn_L8_band.vrt {raster_list}"
        execute_cmd(cmd_vrt)

        logger.info("Starting Clip to S2 extent")
        cmd_clip = f"gdalwarp -te {bnds[0]} {bnds[1]} {bnds[2]} {bnds[3]} {raster_folder}/hrmn_L8_band.vrt {raster_folder}/hrmn_L8_band.tif "
        execute_cmd(cmd_clip)
        upload_name = (
            str(ard_from_key(ref_name, band_num=band_num, s2_tile=s2_tile))
            + f"_{band_num_alias}.tif"
        )
        upload_path = "_".join([production_id, upload_name])

        logger.info("Converting to EWoC ARD")
        if "QA_PIXEL" in band_num:
            if "SR" in band_num:
                get_mask(raster_folder / "hrmn_L8_band.tif")
            raster_to_ard(
                raster_folder / "hrmn_L8_band.tif",
                band_num,
                raster_folder / "hrmn_L8_band_block.tif",
            )
            if not no_upload:
                ewoc_ard_bucket.upload_ard_raster(
                    raster_folder / "hrmn_L8_band_block.tif", upload_path
                )
            up_file_size = (raster_folder / "hrmn_L8_band_block.tif").stat().st_size
        else:
            raster_to_ard(
                raster_folder / "hrmn_L8_band.tif",
                band_num,
                raster_folder / "hrmn_L8_band_block.tif",
                factors,
            )
            if not no_upload:
                ewoc_ard_bucket.upload_ard_raster(
                    raster_folder / "hrmn_L8_band_block.tif", upload_path
                )
            up_file_size = (raster_folder / "hrmn_L8_band_block.tif").stat().st_size
        return 1, up_file_size, upload_path, ewoc_ard_bucket.bucket_name
    except BaseException as e:
        logger.info("Failed for group\n")
        logger.info(tr_group)
        logger.info(e)
        return 0, 0, "", ""
    finally:
        if not debug:
            shutil.rmtree(src_folder)

def process_group(
    tr_group,
    production_id,
    s2_tile,
    out_dir,
    only_sr=False,
    only_sr_mask=False,
    only_tir=False,
    no_upload=False,
    debug=False,
):
    """
    Process a group of Landsat-8 ids, full bands or thermal only
    :param tr_group: A list of s3 ids for Landsat-8 raster on the usgs-landsat bucket
    :param s2_tile: The id of the targeted Sentinel-2 ex 31TCJ (Toulouse)
    :param production_id: Production ID that will be used to upload to s3 bucket
    :type production_id: str
    :param out_dir: Output directory to store the temporary results, should be deleted on full completion
    :param only_sr: Process only SR bands, default to False
    :param only_sr_mask: Process only SR masks, default to False
    :param only_tir: Process only TIR bands, default to False
    :param no_upload: If True the ard files are not uploaded to s3 bucket, default to False
    :param debug: If True all the intermediate files and results will be kept locally, default to False
    :return: Nothing
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
        up_count, up_size, upload_path, bucket_name = process_group_band(
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


def get_band_key(band, tr):
    """
    Get the S3 band id from band name
    :param band: Band number B2/B3/B4/B5/B6/B7/B10/QA
    :param tr: Thermal band s3 id
    :return: date of the product and the s3 key
    """
    sr_bands = ["B2", "B3", "B4", "B5", "B6", "B7"]
    qa_bands = ["QA_PIXEL_SR", "QA_PIXEL_TIR"]
    st_bands = ["B10"]

    base = tr[:-11]
    date = Path(tr).parts[-1].split("_")[3]
    key = None

    if band in st_bands:
        key = f"{base}_ST_{band.upper()}.TIF"
    elif band in sr_bands:
        key = f"{base}_SR_{band.upper()}.TIF"
    elif band in qa_bands:
        key = f"{base}_QA_PIXEL.TIF"
    else:
        ValueError("Band ID {band} not valid!")

    return date, key


if __name__ == "__main__":

    _S2_TILE_ID = "30SVG"

    process_group(
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
