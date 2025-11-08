import logging
from datetime import datetime
from typing import Optional, Any


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


def _num(x: Any) -> Optional[float]:
    """
    安全地将输入转换为浮点数
    
    Args:
        x: 待转换的值
        
    Returns:
        转换后的浮点数，如果无法转换或为空则返回None
    """
    try:
        return float(x) if x not in (None, '') else None
    except Exception:
        return None


def _int(x: Any) -> Optional[int]:
    """
    安全地将输入转换为整数
    
    Args:
        x: 待转换的值
        
    Returns:
        转换后的整数，如果无法转换或为空则返回None
    """
    try:
        return int(x) if x not in (None, '') else None
    except Exception:
        return None


def make_symbol(c: str, m: str) -> str:
    """
    根据市场代码构建股票符号
    
    Args:
        c: 股票代码
        m: 市场代码（SH/SZ/BJ等）
        
    Returns:
        格式化后的股票符号
    """
    m = (m or '').upper()
    if m == 'SH':
        return 'sh' + c
    elif m == 'SZ':
        return 'sz' + c
    elif m == 'BJ':
        return 'bj' + c
    else:
        # 默认处理，根据代码前缀推断市场
        if c.startswith('6'):
            return 'sh' + c
        elif c.startswith('0') or c.startswith('3'):
            return 'sz' + c
        elif c.startswith('8') or c.startswith('4'):
            return 'bj' + c
        return c