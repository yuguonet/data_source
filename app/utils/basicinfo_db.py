# -*- coding: utf-8 -*-
"""Basic info DB stub — 从缓存文件读取A股代码"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_utils import get_active_stock_codes


class _StockBasicDB:
    def market_all_codes(self, status="active"):
        return get_active_stock_codes()


_instance = _StockBasicDB()

def get_stock_basic_db():
    return _instance
