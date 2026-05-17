# -*- coding: utf-8 -*-
"""Normalizer stub — A股代码标准化"""
from typing import Optional

def normalize_cn_code(code: str) -> str:
    """将各种格式的A股代码统一为 SH600519 / SZ000001 格式"""
    code = code.strip().upper()
    if code.startswith(("SH", "SZ", "BJ")):
        return code
    digits = code.lstrip("0") if len(code) > 6 else code
    digits = code  # 保留原始
    if digits.startswith("6"):
        return f"SH{digits}"
    elif digits.startswith(("0", "3", "2")):
        return f"SZ{digits}"
    elif digits.startswith(("4", "8")):
        return f"BJ{digits}"
    return code

def normalize_hk_code(code: str) -> str:
    """港股代码标准化"""
    code = code.strip()
    if code.startswith("HK"):
        return code
    return f"HK{code.zfill(5)}"

def add_market_prefix(code: str, market: str) -> str:
    """给代码加市场前缀"""
    code = code.strip()
    if code.startswith(("SH", "SZ", "BJ", "HK")):
        return code
    if market == "CNStock":
        return normalize_cn_code(code)
    elif market == "HKStock":
        return normalize_hk_code(code)
    return code

def strip_market_prefix(code: str) -> str:
    """去掉市场前缀，返回纯数字（大小写不敏感）"""
    upper = code.upper()
    for prefix in ("SH", "SZ", "BJ", "HK"):
        if upper.startswith(prefix):
            return code[len(prefix):]
    return code

def to_raw_digits(code: str) -> str:
    """转为纯数字代码"""
    return strip_market_prefix(code)

def detect_market(code: str) -> str:
    """检测代码所属市场"""
    if code.startswith("SH") or code.startswith("SZ"):
        return "CNStock"
    if code.startswith("HK"):
        return "HKStock"
    if code.startswith("BJ"):
        return "CNStock"
    return "CNStock"
