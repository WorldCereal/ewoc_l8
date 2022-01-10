import argparse
from datetime import datetime
import logging
from pathlib import Path
from tempfile import gettempdir
import sys

from ewoc_l8 import __version__
from ewoc_l8.l8_process import process_group
from ewoc_l8.utils import json_to_dict

_logger = logging.getLogger(__name__)

# ---- API ----

def _get_default_prod_id()->str:
    str_now=datetime.now().strftime("%Y%m%dT%H%M%S")
    return f"0000_000_{str_now}"

def run_l8_plan(plan_json, out_dir, production_id,
    only_sr=False,
    only_sr_mask=False,
    only_tir=False,
    no_upload=False,
    debug=False):
    """
    Run the Landsat-8 processer over a json plan
    :param plan_json: EWoC WorkPlan in json format
    :param out_dir: Output directory
    :param only_sr: Process only SR bands, default to False
    :param only_sr_mask: Process only SR masks, default to False
    :param only_tir: Process only TIR bands, default to False
    :param no_upload: If True the ard files are not uploaded to s3 bucket, default to False
    :param debug: If True all the intermediate files and results will be kept locally, default to False
    """
    plan = json_to_dict(plan_json)

    if production_id is None:
        _logger.warning("Use computed production id but we must used the one in wp")
        production_id = _get_default_prod_id()

    for s2_tile in plan:
        l8_tirs = plan[s2_tile]["L8_TIRS"]
        for tr_group in l8_tirs:
            process_group(
                tr_group,
                production_id,
                s2_tile,
                out_dir,
                only_sr=only_sr,
                only_sr_mask=only_sr_mask,
                only_tir=only_tir,
                no_upload=no_upload,
                debug=debug,
            )

def run_id(pid_group, s2_tile, out_dir, production_id,
    only_sr=False,
    only_sr_mask=False,
    only_tir=False,
    no_upload=False,
    debug=False):
    """
    Run Landsat-8 processor for one day
    :param pid_group: Landsat-8 group of ids (same date and path)
    :param s2_tile: Sentinel-2 tile id
    :param out_dir: Output directory
    :param only_sr: Process only SR bands, default to False
    :param only_sr_mask: Process only SR masks, default to False
    :param only_tir: Process only TIR bands, default to False
    :param no_upload: If True the ard files are not uploaded to s3 bucket, default to False
    :param debug: If True all the intermediate files and results will be kept locally, default to False
    """


    if production_id is None:
        production_id=_get_default_prod_id()

    process_group(
        pid_group,
        production_id,
        s2_tile,
        str(out_dir),
        only_sr=only_sr,
        only_sr_mask=only_sr_mask,
        only_tir=only_tir,
        no_upload=no_upload,
        debug=debug,
    )

# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.

def parse_args(args):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="Generate EWoC L8 ARD")
    parser.add_argument(
        "--version",
        action="version",
        version=f"ewoc_l8 {__version__}",
    )
    parser.add_argument("-o","--out_dir",
        dest="out_dirpath",
        help="Output Dirpath",
        type=Path,
        default=Path(gettempdir()))
    parser.add_argument("--debug",
        action='store_true',
        help= 'Keep all dirs and intermediates files for debug')
    parser.add_argument("--no-upload",
        action='store_true',
        help= 'Skip the upload of ard files to s3 bucket')
    parser.add_argument("--prod-id",
        dest="prod_id",
        help="Production ID that will be used to upload to s3 bucket, by default it is computed internally")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--only-tir", action='store_true',
        help= 'Process only TIR part of L8 product')
    group.add_argument("--only-sr-mask", action='store_true',
        help= 'Process only SR mask of L8 product')
    group.add_argument("--only-sr", action='store_true',
        help= 'Process only SR part of L8 product')

    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )

    subparsers = parser.add_subparsers(dest='subparser_name')

    parser_prd_ids = subparsers.add_parser('prd_ids',
        help='Generate EWoC L8 ARD from L8 C2 L2 product IDs')
    parser_prd_ids.add_argument(dest="s2_tile_id",
        help="Sentinel-2 Tile ID", type=str)
    parser_prd_ids.add_argument(dest="l8c2l2_prd_ids",
        help="Landsat8 C2 L2 Product ids", nargs='*')

    parser_wp = subparsers.add_parser('wp', help='Generate EWoC L8 ARD from EWoC workplan')
    parser_wp.add_argument(dest="wp",
        help="EWoC workplan in json format",
        type=Path)

    args = parser.parse_args(args)

    if args.subparser_name is None:
        parser.print_help()

    return args

def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )

def main(args):
    """Wrapper allowing :func:`generate_l8_ard` to be called with string arguments in a CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    _logger.debug(args)

    if args.subparser_name == "prd_ids":
        run_id(args.l8c2l2_prd_ids,
            args.s2_tile_id,
            args.out_dirpath,
            args.prod_id,
            only_sr=args.only_sr,
            only_sr_mask=args.only_sr_mask,
            only_tir=args.only_tir,
            no_upload=args.no_upload,
            debug=args.debug)
    elif args.subparser_name == "wp":
        run_l8_plan(args.wp,
            args.out_dirpath,
            args.prod_id,
            only_sr=args.only_sr,
            only_sr_mask=args.only_sr_mask,
            only_tir=args.only_tir,
            no_upload=args.no_upload,
            debug=args.debug)

def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])

if __name__ == "__main__":
    run()
