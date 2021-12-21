import logging
import sys

import click
from ewoc_l8.l8_process import process_group
from ewoc_l8.utils import get_tile_info, json_to_dict

logger = logging.getLogger(__name__)


def set_logger(verbose_v):
    """
    Set the logger level
    :param loglevel:
    :return:
    """
    v_to_level = {"v": "INFO", "vv": "DEBUG"}
    loglevel = v_to_level[verbose_v]
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


@click.group()
@click.option(
    "--verbose",
    type=click.Choice(["v", "vv"]),
    default="vv",
    help="Set verbosity level: v for info, vv for debug",
    required=True,
)
def cli(verbose):
    """
    Landsat-8 processor CLI
    :param verbose: Verbose level
    """
    click.secho("Landsat-8", fg="green", bold=True)
    set_logger(verbose)


@cli.command("l8_plan", help="Landsat-8 with a full plan as input")
@click.option("-p", "--plan_json", help="EWoC Plan in json format")
@click.option("-o", "--out_dir", help="Output directory")
@click.option("--sr/--no-sr", default=False)
@click.option(
    "-d",
    "--debug",
    default=False,
    help="If True all the intermediate files and results will be kept locally",
)
@click.option("--only_sr_mask", is_flag=True, help="Compute only SR masks")
@click.option("--no_tir", is_flag=True, help="Do not compute TIR products")
@click.option(
    "--production_id",
    default="0000",
    help="Production ID that will be used to upload to s3 bucket. " "Default: 0000",
)
def run_l8_plan(plan_json, out_dir, production_id, sr, only_sr_mask, no_tir, debug):
    """
    Run the Landsat-8 processer over a json plan
    :param plan_json: EWoC Plan in json format
    :param out_dir: Output directory
    :param sr: thermal bands and surface reflectance
    :param only_sr_mask: Compute only SR masks
    :param no_tir: Do not compute TIR products
    """
    plan = json_to_dict(plan_json)
    for s2_tile in plan:
        t_srs, bnds = get_tile_info(s2_tile)
        l8_tirs = plan[s2_tile]["L8_TIRS"]
        for tr_group in l8_tirs:
            process_group(
                tr_group,
                t_srs,
                production_id=production_id,
                s2_tile=s2_tile,
                bnds=bnds,
                out_dir=out_dir,
                sr=sr,
                only_sr_mask=only_sr_mask,
                no_tir=no_tir,
                debug=debug,
            )


@cli.command("l8_id", help="Get Landsat-8 product for one id/day")
@click.option(
    "-pid", "--pid_group", help="Landsat-8 group of ids (same date), separeted by space"
)
@click.option("-t", "--s2_tile", help="Sentinel-2 tile id")
@click.option("-o", "--out_dir", help="Output directory")
@click.option("--sr/--no-sr", default=False)
@click.option(
    "-d",
    "--debug",
    default=False,
    help="If True all the intermediate files and results will be kept locally",
)
@click.option("--only_sr_mask", is_flag=True, help="Compute only SR masks")
@click.option("--no_tir", is_flag=True, help="Do not compute TIR products")
@click.option(
    "--production_id",
    default="0000",
    help="Production ID that will be used to upload to s3 bucket. " "Default: 0000",
)
def run_id(pid_group, s2_tile, production_id, out_dir, sr, only_sr_mask, no_tir, debug):
    """
    Run Landsat-8 processor for one day
    :param pid_group: Landsat-8 group of ids (same date), separeted by space
    :param s2_tile: Sentinel-2 tile id
    :param out_dir: Output directory
    :param sr: Get SR bands, default to False
    :param only_sr_mask: Compute only SR masks
    :param no_tir: Do not compute TIR products
    :param debug: If True all the intermediate files and results will be kept locally
    """
    tr_group = pid_group.split(" ")
    t_srs, bnds = get_tile_info(s2_tile)
    process_group(
        tr_group,
        t_srs,
        production_id=production_id,
        s2_tile=s2_tile,
        bnds=bnds,
        out_dir=out_dir,
        sr=sr,
        only_sr_mask=only_sr_mask,
        no_tir=no_tir,
        debug=debug,
    )


if __name__ == "__main__":
    cli()
