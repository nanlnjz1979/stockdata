import logging
from datetime import datetime
from typing import Optional


def norm_date(d: any) -> Optional[str]:
    """
    归一化日期格式
    
    Args:
        d: 日期对象、字符串或其他可转换为日期的对象
        
    Returns:
        格式化后的日期字符串（YYYYMMDD格式），如果无法转换则返回None
    """
    if not d:
        return None
    if isinstance(d, datetime):
        return d.strftime("%Y%m%d")
    s = str(d)
    try:
        if '-' in s:
            s = s.replace('-', '')
        return s
    except Exception:
        return None