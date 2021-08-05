import json
import os
from eotile.eotile_module import main

import boto3

def json_to_dict(path_to_json):
    with open(path_to_json) as f:
        data = json.load(f)
    return data

def make_dir(fold_dir):
    if not os.path.exists(fold_dir):
        os.makedirs(fold_dir)

def ard_from_key(key,s2_tile,out_dir=None):
    product_id = os.path.split(key)[-1]
    platform = product_id.split('_')[0]
    processing_level = product_id.split('_')[1]
    date = product_id.split('_')[3]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = s2_tile
    unique_id = f"{product_id.split('_')[2]}{product_id.split('_')[5]}{product_id.split('_')[6]}"
    folder_st = os.path.join('TIR', tile_id[:2], tile_id[2], tile_id[3:], year,date.split('T')[0])
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    out_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    raster_fn = os.path.join(folder_st, dir_name, out_name)
    if out_dir is not None:
        tmp = os.path.join(out_dir, folder_st, dir_name)
        if not os.path.exists(tmp):
            os.makedirs(tmp)
    return raster_fn



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
def get_tile_proj(s2tile):
    res = main(s2tile)[0]
    srs = res['SRS'].values
    return srs[0]
