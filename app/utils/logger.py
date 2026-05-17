# -*- coding: utf-8 -*-
"""Logger stub — 输出到 stdout"""
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
)

def get_logger(name: str):
    return logging.getLogger(name)
