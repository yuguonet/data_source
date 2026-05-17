# -*- coding: utf-8 -*-
"""Trading calendar stub — 简单实现"""
from datetime import datetime, timedelta

def trading_days_count(start_date: str, end_date: str) -> int:
    """粗略计算交易日数（不含周末）"""
    fmt = "%Y-%m-%d"
    s = datetime.strptime(start_date, fmt)
    e = datetime.strptime(end_date, fmt)
    count = 0
    while s <= e:
        if s.weekday() < 5:
            count += 1
        s += timedelta(days=1)
    return max(count, 1)
