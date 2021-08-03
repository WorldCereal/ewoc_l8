import json
import os

import rasterio
from rasterio.merge import merge

def json_to_dict(path_to_json):
    with open(path_to_json) as f:
        data = json.load(f)
    return data



def ard_from_key(key,s2_tile,out_dir):
    product_id = os.path.split(key)[-1]
    platform = product_id.split('_')[0]
    processing_level = product_id.split('_')[1]
    date = product_id.split('_')[3]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = s2_tile
    out_dir = os.path.join(out_dir,'TIR')
    unique_id = f"{product_id.split('_')[2]}{product_id.split('_')[5]}{product_id.split('_')[6]}"
    folder_st = os.path.join(out_dir, tile_id[:2], tile_id[2], tile_id[3:], year,date.split('T')[0])
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    out_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    raster_fn = os.path.join(folder_st, dir_name, out_name)
    tmp = os.path.join(folder_st, dir_name)
    if not os.path.exists(tmp):
        os.makedirs(tmp)
    return raster_fn

def merge_rasters(rasters,bounds,output_fn):
    """
    Merge a list of rasters and clip using bounds
    :param rasters: List of raster paths
    :param bounds: Bounds from get_bounds()
    :param output_fn: Full path and name of the mosaic
    """
    sources = []
    for raster in rasters:
        src = rasterio.open(raster)
        sources.append(src)
    merge(sources,dst_path=output_fn,method='max',bounds=bounds)
    for src in sources:
        src.close()