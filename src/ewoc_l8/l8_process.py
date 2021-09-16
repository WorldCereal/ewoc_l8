import logging
import os
import shutil

import rasterio
from ewoc_l8.utils import ard_from_key,make_dir, binary_sr_qa, key_from_id
from dataship.dag.utils import download_s3file
from dataship.dag.s3man import upload_file, get_s3_client

logger = logging.getLogger(__name__)

def process_group_band(band_num,tr_group,t_srs,s2_tile,bnds,res,out_dir,debug):
    """
    Process Landsat-8 band: Download, merge and clip to S2 tile footprint
    For one band, one date
    :param band_num: Landsat-8 band name, accepted values: ['B2','B3','B4','B5','B6','B7','B10','QA']
    :param tr_group: A list of s3 ids for Landsat-8 raster on the usgs-landsat bucket
    :param t_srs: Target projection system, to determined from the Sentinel-2 tile projection
    :param s2_tile: The id of the targeted Sentinel-2 ex 31TCJ (Toulouse)
    :param bnds: Extent of the Sentinel-2 tile, you can get this using the function get_bounds from dataship/ewoc_dag
    :param res: Resampling resolution, could be 10 or 20 meters
    :param out_dir: Output directory to store the temporary results, should be deleted on full completion
    :param debug: If True all the intermediate files and results will be kept locally
    :return: Nothing
    """
    # Create list of same bands but different dates
    l8_to_s2={'B2':'B02','B3':'B03','B4':'B04','B5':'B08','B6':'B11','B7':'B12','B10':'B10','QA':'QA'}
    if band_num == "QA_AEROSOL":
        sr_method = "near"
        band_num_alias = "MASK"
    else:
        sr_method = "bilinear"
        band_num_alias = l8_to_s2[band_num]
    bucket = "usgs-landsat"
    prefix = os.getenv("DEST_PREFIX")
    group_bands = []
    s3c = get_s3_client()
    for tr in tr_group:
        tr = key_from_id(tr)
        date,key = get_band_key(band_num,tr)
        group_bands.append(key)
    tmp_folder = os.path.join(out_dir,"tmp",date, band_num)
    src_folder = os.path.join(out_dir,"tmp")
    make_dir(tmp_folder)
    group_bands = list(set(group_bands))
    group_bands.sort()
    ref_name = group_bands[0]
    for band in group_bands:
        raster_fn = os.path.join(tmp_folder,os.path.split(band)[-1][:-11])
        download_s3file(band, raster_fn, bucket)
    try:
        logging.info("Starting Re-projection")
        for raster in os.listdir(tmp_folder):
            raster = os.path.join(tmp_folder, raster)
            if res is not None:
                cmd_proj = f"gdalwarp -tr {res} {res} -r {sr_method} -t_srs {t_srs} {raster} {raster[:-4]}_r.tif"
            else:
                cmd_proj = f"gdalwarp -t_srs {t_srs} {raster} {raster[:-4]}_r.tif"
            os.system(cmd_proj)
        raster_list = " ".join([os.path.join(tmp_folder, rst) for rst in os.listdir(tmp_folder) if rst.endswith('_r.tif')])
        logging.info("Starting VRT creation")
        cmd_vrt = f"gdalbuildvrt -q {tmp_folder}/hrmn_L8_band.vrt {raster_list}"
        os.system(cmd_vrt)
        logging.info("Starting Clip to S2 extent")
        cmd_clip = f"gdalwarp -te {bnds[0]} {bnds[1]} {bnds[2]} {bnds[3]} {tmp_folder}/hrmn_L8_band.vrt {tmp_folder}/hrmn_L8_band.tif "
        os.system(cmd_clip)
        upload_name = ard_from_key(ref_name,band_num=band_num, s2_tile=s2_tile) + f'_{band_num_alias}.tif'
        logging.info("Converting to EWoC ARD")
        if band_num == "QA_AEROSOL":
            binary_sr_qa(os.path.join(tmp_folder, 'hrmn_L8_band.tif'))
            upload_file(s3c, os.path.join(tmp_folder, 'hrmn_L8_band.tif'), "world-cereal",
                        os.path.join(prefix, upload_name))
        else:
            raster_to_ard(os.path.join(tmp_folder, 'hrmn_L8_band.tif'),band_num,os.path.join(tmp_folder, 'hrmn_L8_band_block.tif'))
            upload_file(s3c, os.path.join(tmp_folder, 'hrmn_L8_band_block.tif'), "world-cereal", os.path.join(prefix, upload_name))
    except:
        logging.info('Failed for group\n')
        logging.info(tr_group)
    finally:
        if not debug:
            shutil.rmtree(src_folder)

def process_group(tr_group,t_srs,s2_tile, bnds,out_dir,sr,debug):
    """
    Process a group of Landsat-8 ids, full bands or thermal only
    :param tr_group: A list of s3 ids for Landsat-8 raster on the usgs-landsat bucket
    :param t_srs: Target projection system, to determined from the Sentinel-2 tile projection
    :param s2_tile: The id of the targeted Sentinel-2 ex 31TCJ (Toulouse)
    :param bnds: Extent of the Sentinel-2 tile, you can get this using the function get_bounds from dataship/ewoc_dag
    :param out_dir: Output directory to store the temporary results, should be deleted on full completion
    :param sr: Set to True to get all the following bands B2/B3/B4/B5/B6/B7/B10/QA, False by default
    :param debug: If True all the intermediate files and results will be kept locally
    :return: Nothing
    """
    res_dict={'B2':'10','B3':'10','B4':'10','B5':'10','B6':'20','B7':'20','B10':None,'QA':None,'QA_AEROSOL':'20'}
    if sr:
        process_bands = ['QA_AEROSOL', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B10', 'QA']
    else:
        process_bands = ['B10', 'QA']

    for band in process_bands:
        logging.info(f'Processing {band}')
        process_group_band(band,tr_group,t_srs,s2_tile,bnds,res = res_dict[band], out_dir=out_dir,debug=debug)


def get_band_key(band,tr):
    """
    Get the S3 band id from band name
    :param band: Band number B2/B3/B4/B5/B6/B7/B10/QA
    :param tr: Thermal band s3 id
    :return: date of the product and the s3 key
    """
    sr_bands = ['B2','B3','B4','B5','B6','B7','QA_AEROSOL']
    st_bands = ['B10','QA']
    base = tr[:-11]
    date = os.path.split(tr)[-1].split('_')[3]
    key = None
    if band in st_bands:
        key = f"{base}_ST_{band.upper()}.TIF"
    elif band in sr_bands:
        key = f"{base}_SR_{band.upper()}.TIF"
    else:
        logging.info("Band not found")
    return date, key

def raster_to_ard(raster_path, band_num, raster_fn):
    """
    Read raster and update internals to fit ewoc ard specs
    :param raster_path: Path to raster file
    :param band_num: Band number, B02 for example
    :param raster_fn: Output raster path
    """
    with rasterio.Env(GDAL_CACHEMAX=2048):
        with rasterio.open(raster_path,'r') as src:
            raster_array = src.read()
            meta = src.meta.copy()
    meta["driver"] = "GTiff"
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

if __name__ == "__main__":

    from dataship.dag.utils import get_bounds
    s2_tile = "30SVG"
    t_srs = "EPSG:32730"
    bnds = get_bounds(s2_tile)
    out_dir = "."
    tr_group = [
        "s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2019/200/035/LC08_L2SP_200035_20190321_20200829_02_T1/LC08_L2SP_200035_20190321_20200829_02_T1_ST_B10.TIF",
        "s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2019/200/034/LC08_L2SP_200034_20190321_20200829_02_T1/LC08_L2SP_200034_20190321_20200829_02_T1_ST_B10.TIF"]
    # process_group_band("B2",tr_group, t_srs, s2_tile, bnds, out_dir)
    # Run a full (SR + TIR) test with debug mode
    process_group(tr_group, t_srs, s2_tile, bnds, out_dir, sr=True,debug=True)
