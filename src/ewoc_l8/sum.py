from l8_process import process_group_band, process_group
from dataship.dag.utils import get_bounds
s2_tile = "30SVG"
t_srs = "EPSG:32730"
bnds = get_bounds(s2_tile)
out_dir = "."
tr_group =["s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2019/200/035/LC08_L2SP_200035_20190321_20200829_02_T1/LC08_L2SP_200035_20190321_20200829_02_T1_ST_B10.TIF","s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2019/200/034/LC08_L2SP_200034_20190321_20200829_02_T1/LC08_L2SP_200034_20190321_20200829_02_T1_ST_B10.TIF"]
#process_group_band("B2",tr_group, t_srs, s2_tile, bnds, out_dir)
process_group(tr_group,t_srs,s2_tile,bnds,out_dir,only_tir=False)
