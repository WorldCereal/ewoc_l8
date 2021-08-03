import os

import click
from utils import *
from dataship.dag.utils import get_bounds
from dataship.dag.s3man import upload_file, get_s3_client


@click.group()
def cli():
    click.secho("Landsat-8", fg="green", bold=True)


@cli.command('l8_plan', help="Landsat-8 with a full plan as input")
@click.option('-p', '--plan_json', help="EWoC Plan in json format")
@click.option('-o', '--out_dir', default=None,help="Output directory")
def run_l8_plan(plan_json, out_dir):
    # For each tile
    # Download all the products in a work folder
    plan = json_to_dict(plan_json)
    for s2_tile in plan:
        print(s2_tile)
        l8_tirs = plan[s2_tile]['L8_TIRS']
        for tr_group in l8_tirs:
            tmp_group = os.path.join(out_dir,"w_dir")
            if not os.path.exists(tmp_group):
                os.makedirs(tmp_group)
            b10 = []
            qa = []
            for tr_day in tr_group:
                s3_key = tr_day
                if not s3_key in b10:
                    b10.append(s3_key)
                    qa.append(s3_key.replace('ST_B10','ST_QA'))
            b10_mosa = ard_from_key(b10[0],s2_tile,out_dir)+'_B10.tif'
            qa_mosa = ard_from_key(qa[0],s2_tile,out_dir)+'_QA.tif'
            bnds = get_bounds(s2_tile)
            if len(b10)>1:
                try:
                    merge_rasters(b10,bnds,b10_mosa)
                    merge_rasters(qa,bnds,qa_mosa)
                    s3c = get_s3_client()
                    s3_obj_b10 = b10_mosa.split('/')[1:]
                    s3_obj_b10 = "/".join(s3_obj_b10)
                    s3_obj_b10 = "WORLDCEREAL_PREPROC/test_tir/"+s3_obj_b10
                    upload_file(s3c,b10_mosa,"world-cereal",s3_obj_b10)
                    s3_obj_qa = qa_mosa.split('/')[1:]
                    s3_obj_qa = "/".join(s3_obj_qa)
                    s3_obj_qa = "WORLDCEREAL_PREPROC/test_tir/"+s3_obj_qa
                    upload_file(s3c,qa_mosa,"world-cereal",s3_obj_qa)
                except:
                    print('Error')
                    print(b10)


if __name__ == '__main__':
    cli()

