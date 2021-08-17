import os
import shutil

import rasterio
from ewoc_l8.utils import ard_from_key,make_dir
from dataship.dag.utils import download_s3file
from dataship.dag.s3man import upload_file, get_s3_client

def process_group_band(band_num,tr_group,t_srs,s2_tile,bnds,res,out_dir):
    # Create list of same bands but different dates
    bucket = "usgs-landsat"
    prefix = os.getenv("DEST_PREFIX")
    group_bands = []
    s3c = get_s3_client()
    for tr in tr_group:
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
        for raster in os.listdir(tmp_folder):
            raster = os.path.join(tmp_folder, raster)
            if res is not None:
                cmd_proj = f"gdalwarp -tr {res} {res} -t_srs {t_srs} {raster} {raster[:-4]}_r.tif"
            else:
                cmd_proj = f"gdalwarp -t_srs {t_srs} {raster} {raster[:-4]}_r.tif"
            os.system(cmd_proj)
        raster_list = " ".join([os.path.join(tmp_folder, rst) for rst in os.listdir(tmp_folder) if rst.endswith('_r.tif')])
        cmd_vrt = f"gdalbuildvrt -q {tmp_folder}/hrmn_L8_band.vrt {raster_list}"
        os.system(cmd_vrt)
        cmd_clip = f"gdalwarp -te {bnds[0]} {bnds[1]} {bnds[2]} {bnds[3]} {tmp_folder}/hrmn_L8_band.vrt {tmp_folder}/hrmn_L8_band.tif "
        os.system(cmd_clip)
        upload_name = ard_from_key(ref_name, s2_tile) + f'_{band_num}.tif'
        raster_to_ard(os.path.join(tmp_folder, 'hrmn_L8_band.tif'),band_num,os.path.join(tmp_folder, 'hrmn_L8_band_block.tif'))
        upload_file(s3c, os.path.join(tmp_folder, 'hrmn_L8_band_block.tif'), "world-cereal", os.path.join(prefix, upload_name))
        shutil.rmtree(src_folder)
    except:
        print('Failed for group\n')
        print(tr_group)

def process_group(tr_group,t_srs,s2_tile, bnds,out_dir,only_tir=True):
    res_dict={'B2':'10','B3':'10','B4':'10','B5':'10','B6':'20','B7':'20','B10':None,'QA':None}
    if only_tir:
        process_bands = ['B10','QA']
    else:
        process_bands = ['B2','B3','B4','B5','B6','B7','B10','QA']
    for band in process_bands:
        print(res_dict[band])
        process_group_band(band,tr_group,t_srs,s2_tile,bnds,res_dict[band],out_dir)


def get_band_key(band,tr):
    #s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2019/200/034/LC08_L2SP_200034_20190217_20200829_02_T1/LC08_L2SP_200034_20190217_20200829_02_T1_ST_B10.TIF
    sr_bands = ['B2','B3','B4','B5','B6','B7']
    st_bands = ['B10','QA']
    base = tr[:-11]
    date = os.path.split(tr)[-1].split('_')[3]
    key = None
    if band in st_bands:
        key = f"{base}_ST_{band.upper()}.TIF"
    elif band in sr_bands:
        key = f"{base}_SR_{band.upper()}.TIF"
    else:
        print("Band not found")
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
    print(blocksize)
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