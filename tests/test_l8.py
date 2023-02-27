# -*- coding: utf-8 -*-
""" Test EWoC classif
"""
from pathlib import Path
import unittest
from tempfile import gettempdir
from ewoc_l8.l8_process import generate_l8_band_ard, generate_l8_ard, get_band_key
from ewoc_dag.utils import get_bounds
from ewoc_l8.utils import get_tile_info
class TestClassif(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_band_key(self):
        """Nominal case with st_band B10
        """
        date, key=get_band_key(
            "B10", 
            "LC08_L2SP_199029_20211216_20211223_02_T1")
        print(f"date : {date} and key : {key}")

    def test_generate_l8_ard_31TCJ(self):
        """Nominal case with 31TCJ, test also generate_l8_band_ard() by calling the function
        """
        generate_l8_ard(
        ["LC08_L2SP_199029_20211216_20211223_02_T1",
         "LC08_L2SP_199030_20211216_20211223_02_T1"],
        "c728b264-5c97-4f4c-81fe-1500d4c4dfbd_46172_20221025141122",
        "31TCJ",
        Path(gettempdir()),
        only_sr=True,
        only_sr_mask=False,
        only_tir=False,
        no_upload=True,
        debug=True,
        )

    def test_generate_l8_band_ard_31TCJ(self):
        """Nominal case with 31TCJ
        """
        s2_tile="31TCJ"
        bnds=get_bounds(s2_tile)
        srs, bb=get_tile_info(s2_tile)
        generate_l8_band_ard(
        'B10', 
        ["LC08_L2SP_199029_20211216_20211223_02_T1",
         "LC08_L2SP_199030_20211216_20211223_02_T1"],
        "c728b264-5c97-4f4c-81fe-1500d4c4dfbd_46172_20221025141122",
        t_srs=srs,
        bnds=bnds,
        s2_tile="31TCJ",
        out_dir=Path(gettempdir()),
        res="10",
        no_upload=True,
        debug=True,
        )
        