import os

import click
import shutil
from ewoc_l8.utils import *
from dataship.dag.utils import get_bounds
from dataship.dag.s3man import get_s3_client,upload_file


@click.group()
def cli():
    click.secho("Landsat-8", fg="green", bold=True)


@cli.command('l8_plan', help="Landsat-8 with a full plan as input")
@click.option('-p', '--plan_json', help="EWoC Plan in json format")
@click.option('-o', '--out_dir',help="Output directory")
def run_l8_plan(plan_json, out_dir):
    s3c = get_s3_client()
    bucket = "world-cereal"
    prefix = os.getenv("DEST_PREFIX")
    plan = json_to_dict(plan_json)
    for s2_tile in plan:
        t_srs = get_tile_proj(s2_tile)
        l8_tirs = plan[s2_tile]['L8_TIRS']
        bnds = get_bounds(s2_tile)
        for tr_group in l8_tirs:
            b10 = []
            qa = []
            date = ""
            for tr in tr_group:
                date = os.path.split(tr)[-1].split('_')[3]
                b10.append(tr)
                qa.append(tr.replace('ST_B10','ST_QA'))
            b10_tmp_folder = os.path.join(out_dir,"tmp",date,'B10')
            qa_tmp_folder = os.path.join(out_dir,"tmp",date,'qa')
            make_dir(b10_tmp_folder)
            make_dir(qa_tmp_folder)
            # Remove duplicates
            b10 = list(set(b10))
            qa = list(set(qa))
            for b in b10:
                b10_name = os.path.split(b)[-1]
                download_s3file(b,os.path.join(b10_tmp_folder,b10_name),'usgs-landsat')
            for q in qa:
                qa_name = os.path.split(q)[-1]
                download_s3file(q,os.path.join(qa_tmp_folder,qa_name),'usgs-landsat')
            try:
                ref_name = b10[0]
                ## B10
                for raster in os.listdir(b10_tmp_folder):
                    raster = os.path.join(b10_tmp_folder,raster)
                    cmd_proj = f"gdalwarp -t_srs {t_srs} {raster} {raster[:-4]}_r.tif"
                    os.system(cmd_proj)
                raster_list = " ".join([os.path.join(b10_tmp_folder, rst) for rst in os.listdir(b10_tmp_folder) if rst.endswith('_r.tif')])
                cmd_vrt = f"gdalbuildvrt {b10_tmp_folder}/hrmn_L8_b10.vrt {raster_list}"
                os.system(cmd_vrt)
                cmd_clip =f"gdalwarp -te {bnds[0]} {bnds[1]} {bnds[2]} {bnds[3]} {b10_tmp_folder}/hrmn_L8_b10.vrt {b10_tmp_folder}/hrmn_L8_b10.tif "
                os.system(cmd_clip)
                b10_upload_name = ard_from_key(ref_name,s2_tile)+'_B10.tif'
                upload_file(s3c,os.path.join(b10_tmp_folder,'hrmn_L8_b10.tif'),bucket,os.path.join(prefix,b10_upload_name))
                shutil.rmtree(b10_tmp_folder)

                ## QA
                for raster in os.listdir(qa_tmp_folder):
                    raster = os.path.join(qa_tmp_folder,raster)
                    cmd_proj = f"gdalwarp -t_srs {t_srs} {raster} {raster[:-4]}_r.tif"
                    os.system(cmd_proj)
                raster_list = " ".join([os.path.join(qa_tmp_folder, rst) for rst in os.listdir(qa_tmp_folder) if rst.endswith('_r.tif')])
                cmd_vrt = f"gdalbuildvrt {qa_tmp_folder}/hrmn_L8_qa.vrt {raster_list}"
                os.system(cmd_vrt)
                cmd_clip =f"gdalwarp -te {bnds[0]} {bnds[1]} {bnds[2]} {bnds[3]} {qa_tmp_folder}/hrmn_L8_qa.vrt {qa_tmp_folder}/hrmn_L8_qa.tif "
                os.system(cmd_clip)
                qa_upload_name = ard_from_key(ref_name,s2_tile)+'_QA.tif'
                upload_file(s3c,os.path.join(qa_tmp_folder,'hrmn_L8_qa.tif'),bucket,os.path.join(prefix,qa_upload_name))
                shutil.rmtree(qa_tmp_folder)
                shutil.rmtree(os.path.join(out_dir, 'tmp',date))
            except:
                print('Failed for group\n')
                print(tr_group)


if __name__ == '__main__':
    cli()

