import logging
import os
import shutil

from dataship.dag.utils import download_s3file
from dataship.dag.s3man import upload_file, get_s3_client
import rasterio

from ewoc_l8.utils import ard_from_key,make_dir, get_mask, key_from_id, raster_to_ard

logger = logging.getLogger(__name__)

def process_group_band(band_num,tr_group,t_srs,s2_tile,bnds,res,out_dir,debug):
    """
    Process Landsat-8 band: Download, merge and clip to S2 tile footprint
    For one band, one date
    :param band_num: Landsat-8 band name, accepted values: ['B2','B3','B4','B5','B6','B7','B10','QA','QA_PIXEL']
    :param tr_group: A list of s3 ids for Landsat-8 raster on the usgs-landsat bucket
    :param t_srs: Target projection system, to determined from the Sentinel-2 tile projection
    :param s2_tile: The id of the targeted Sentinel-2 ex 31TCJ (Toulouse)
    :param bnds: Extent of the Sentinel-2 tile, you can get this using the function get_bounds from dataship/ewoc_dag
    :param res: Resampling resolution, could be 10 or 20 meters
    :param sr: Set to True to get all the following bands B2/B3/B4/B5/B6/B7/B10/QA, False by default
    :param out_dir: Output directory to store the temporary results, should be deleted on full completion
    :param debug: If True all the intermediate files and results will be kept locally
    :return: Nothing
    """
    # Create list of same bands but different dates
    l8_to_s2={'B2':'B02','B3':'B03','B4':'B04','B5':'B08','B6':'B11','B7':'B12','B10':'B10','QA_PIXEL_SR':'MASK',
              'QA_PIXEL_TIR':'QA_PIXEL'}

    s2_scaling_factor = 10000
    factors = {'a': 0.0000275 * s2_scaling_factor,
               'b': -0.2 * s2_scaling_factor,
               }  # Scaling factors

    if band_num in ["QA_PIXEL_TIR", "QA_PIXEL_SR"]:
        sr_method = "near"
        dst_nodata = "-dstnodata 1"
    else:
        sr_method = "bilinear"
        dst_nodata=""
    band_num_alias = l8_to_s2[band_num]
    bucket = "usgs-landsat"
    bucket_name = "world-cereal"
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
        logger.info("Starting Re-projection")
        for raster in os.listdir(tmp_folder):
            raster = os.path.join(tmp_folder, raster)
            if res is not None:
                cmd_proj = f"gdalwarp -tr {res} {res} -r {sr_method} -t_srs {t_srs} {raster} {raster[:-4]}_r.tif {dst_nodata}"
            else:
                cmd_proj = f"gdalwarp -t_srs {t_srs} {raster} {raster[:-4]}_r.tif {dst_nodata}"
            os.system(cmd_proj)
        raster_list = " ".join([os.path.join(tmp_folder, rst) for rst in os.listdir(tmp_folder) if rst.endswith('_r.tif')])
        logger.info("Starting VRT creation")
        cmd_vrt = f"gdalbuildvrt -q {tmp_folder}/hrmn_L8_band.vrt {raster_list}"
        os.system(cmd_vrt)
        logger.info("Starting Clip to S2 extent")
        cmd_clip = f"gdalwarp -te {bnds[0]} {bnds[1]} {bnds[2]} {bnds[3]} {tmp_folder}/hrmn_L8_band.vrt {tmp_folder}/hrmn_L8_band.tif "
        os.system(cmd_clip)
        upload_name = ard_from_key(ref_name,band_num=band_num, s2_tile=s2_tile) + f'_{band_num_alias}.tif'
        upload_path = os.path.join(prefix, upload_name)
        logger.info("Converting to EWoC ARD")
        if "QA_PIXEL" in band_num:
            if "SR" in band_num:
                get_mask(os.path.join(tmp_folder, 'hrmn_L8_band.tif'))
            raster_to_ard(os.path.join(tmp_folder, 'hrmn_L8_band.tif'), band_num,
                          os.path.join(tmp_folder, 'hrmn_L8_band_block.tif'))
            upload_file(s3c, os.path.join(tmp_folder, 'hrmn_L8_band_block.tif'), bucket_name,
                        os.path.join(prefix, upload_name))
            up_file_size = os.path.getsize(os.path.join(tmp_folder, 'hrmn_L8_band_block.tif'))
        else:
            raster_to_ard(os.path.join(tmp_folder, 'hrmn_L8_band.tif'),
                          band_num,
                          os.path.join(tmp_folder, 'hrmn_L8_band_block.tif'),
                          factors
                          )
            upload_file(s3c, os.path.join(tmp_folder, 'hrmn_L8_band_block.tif'), bucket_name, upload_path)
            up_file_size = os.path.getsize(os.path.join(tmp_folder, 'hrmn_L8_band_block.tif'))
        return 1, up_file_size, upload_path, bucket_name
    except:
        logger.info('Failed for group\n')
        logger.info(tr_group)
        return 0,0, "", ""
    finally:
        if not debug:
            shutil.rmtree(src_folder)


def process_group(tr_group, t_srs, s2_tile, bnds, out_dir, sr, only_sr_mask, no_tir, debug):
    """
    Process a group of Landsat-8 ids, full bands or thermal only
    :param tr_group: A list of s3 ids for Landsat-8 raster on the usgs-landsat bucket
    :param t_srs: Target projection system, to determined from the Sentinel-2 tile projection
    :param s2_tile: The id of the targeted Sentinel-2 ex 31TCJ (Toulouse)
    :param bnds: Extent of the Sentinel-2 tile, you can get this using the function get_bounds from dataship/ewoc_dag
    :param out_dir: Output directory to store the temporary results, should be deleted on full completion
    :param sr: Set to True to get all the following bands B2/B3/B4/B5/B6/B7/B10/QA, False by default
    :param debug: If True all the intermediate files and results will be kept locally
    :param only_sr_mask: Compute only SR masks
    :param no_tir: Do not compute TIR products
    :return: Nothing
    """
    res_dict={'B2':'10','B3':'10','B4':'10','B5':'10','B6':'20','B7':'20','B10':'30','QA_PIXEL_SR':'20',
              'QA_PIXEL_TIR':'30'}
    if sr:
        process_bands = ['QA_PIXEL_TIR', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B10', 'QA_PIXEL_SR']
    elif only_sr_mask:
        process_bands = ['QA_PIXEL_SR']
    elif no_tir:
        process_bands = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'QA_PIXEL_SR']
    else:
        process_bands = ['B10', 'QA_PIXEL_TIR']
    upload_count = 0
    total_size = 0
    paths = []
    for band in process_bands:
        logger.info('Processing %s', band)
        up_count, up_size, upload_path, bucket_name = process_group_band(band,tr_group,t_srs,s2_tile,bnds,
                                                                         res = res_dict[band],
                                                                         out_dir=out_dir,debug=debug)
        upload_count+=up_count
        total_size+=up_size
        if os.path.dirname(upload_path) not in paths:
            paths.append(os.path.dirname(upload_path))

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
        path_list.append(f's3://{bucket_name}/{optic_path}')

    logging_string = f'Uploaded {upload_count} tif files to bucket |'+ (' ; '.join(path_list))
    print(logging_string)


def get_band_key(band,tr):
    """
    Get the S3 band id from band name
    :param band: Band number B2/B3/B4/B5/B6/B7/B10/QA
    :param tr: Thermal band s3 id
    :return: date of the product and the s3 key
    """
    sr_bands = ['B2','B3','B4','B5','B6','B7']
    qa_bands=['QA_PIXEL_SR', 'QA_PIXEL_TIR']
    st_bands = ['B10',]
    base = tr[:-11]
    date = os.path.split(tr)[-1].split('_')[3]
    key = None
    if band in st_bands:
        key = f"{base}_ST_{band.upper()}.TIF"
    elif band in sr_bands:
        key = f"{base}_SR_{band.upper()}.TIF"
    elif band in qa_bands:
        key = f"{base}_QA_PIXEL.TIF"
    else:
        logging.info("Band not found")
    return date, key


if __name__ == "__main__":

    from dataship.dag.utils import get_bounds
    s2_tile = "30SVG"
    t_srs = "EPSG:32730"
    bnds = get_bounds(s2_tile)
    out_dir = "."
    tr_group = [
        "LC08_L1TP_201035_20191022_20200825_02_T1", "LC08_L1TP_201034_20191022_20200825_02_T1"]
    # process_group_band("B2",tr_group, t_srs, s2_tile, bnds, out_dir)
    # Run a full (SR + TIR) test with debug mode
    process_group(tr_group, t_srs, s2_tile, bnds, out_dir, sr=True, only_sr_mask=False, no_tir=False, debug=True)
