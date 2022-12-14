import argparse
from datetime import datetime
import json
import logging
from pathlib import Path
import sys
from tempfile import gettempdir
from typing import List

from ewoc_l8 import __version__
from ewoc_l8.l8_process import L8ARDProcessorBaseError, generate_l8_ard

_logger = logging.getLogger(__name__)

class L8ARDProcessorError(Exception):
    """Exception raised for errors in the L8 ARD generation."""

    def __init__(self, s2_tile_id, l8c2l2_prd_ids, exit_code):
        self._s2_tile_id = s2_tile_id
        self._l8c2l2_prd_ids = l8c2l2_prd_ids
        self.exit_code = exit_code
        self._message = "Error during L8 ARD generation:"
        super().__init__(self._message)

    def __str__(self):
        return f"{self._message} No L8 ARD on {self._s2_tile_id} for {self._l8c2l2_prd_ids} !"

# ---- API ----

def _get_default_prod_id()->str:
    str_now=datetime.now().strftime("%Y%m%dT%H%M%S")
    return f"0000_000_{str_now}"

def generate_l8_ard_from_pids(
    pid_group: List[str],
    s2_tile: str,
    out_dir: Path,
    production_id: str,
    only_sr: bool = False,
    only_sr_mask: bool = False,
    only_tir: bool = False,
    no_upload: bool = False,
    debug: bool = False)->None:
    """
    Run Landsat-8 processor for one day
    :param pid_group: Landsat-8 group of ids (same date and path)
    :param s2_tile: Sentinel-2 tile id
    :param out_dir: Output directory
    :param production_id: Production ID that will be used to upload to s3 bucket
    :param only_sr: Process only SR bands, default to False
    :param only_sr_mask: Process only SR masks, default to False
    :param only_tir: Process only TIR bands, default to False
    :param no_upload: If True the ard files are not uploaded to s3 bucket, default to False
    :param debug: If True all the intermediate files and results will be kept locally,
         default to False
    """

    if production_id is None:
        production_id=_get_default_prod_id()

    try:
        upload_count, path_list = generate_l8_ard(
            pid_group,
            production_id,
            s2_tile,
            out_dir,
            only_sr=only_sr,
            only_sr_mask=only_sr_mask,
            only_tir=only_tir,
            no_upload=no_upload,
            debug=debug,
        )
    except L8ARDProcessorBaseError as exc:
        _logger.error(exc)
        raise L8ARDProcessorError(s2_tile, pid_group, exc.exit_code) from exc

# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.

def parse_args(arguments: List[str])->argparse.Namespace:
    """Parse command line parameters

    Args:
      arguments: command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="Generate EWoC L8 ARD")
    parser.add_argument(dest="s2_tile_id",
        help="Sentinel-2 Tile ID", 
        type=str)
    parser.add_argument(dest="l8c2l2_prd_ids",
        help="Landsat8 C2 L2 Product ids", 
        nargs='*')
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
        help="Production ID that will be used as prefix for the path used to save "\
            " the results into the s3 bucket"\
            ", by default it is computed internally")

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

    args = parser.parse_args(arguments)

    return args

def setup_logging(loglevel:int)->None:
    """Setup basic logging

    Args:
      loglevel: minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )

def main(arguments: List[str])->None:
    """Wrapper allowing :func:`generate_l8_ard` to be called with string arguments in a CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      arguments: command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    args = parse_args(arguments)
    setup_logging(args.loglevel)
    _logger.debug(args)

    _logger.debug("Starting Generate L8 ARD for %s over %s MGRS Tile ...",
        args.l8c2l2_prd_ids, args.s2_tile_id)

    try:
        generate_l8_ard_from_pids(args.l8c2l2_prd_ids,
            args.s2_tile_id,
            args.out_dirpath,
            args.prod_id,
            only_sr=args.only_sr,
            only_sr_mask=args.only_sr_mask,
            only_tir=args.only_tir,
            no_upload=args.no_upload,
            debug=args.debug)
    except L8ARDProcessorError as exc:
        _logger.critical(exc)
        sys.exit(exc.exit_code)
    else:
        _logger.info("Generation of L8 ARD for %s over %s MGRS Tile is ended!",
            args.l8c2l2_prd_ids, args.s2_tile_id)

def run()->None:
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])

if __name__ == "__main__":
    run()
