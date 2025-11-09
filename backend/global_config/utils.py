import logging
from datetime import datetime
from typing import Optional, Any
from datetime import date, timedelta
from typing import Optional, Union


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


def make_symbol(c: str, m: str = '') -> str:
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
        # 根据股票代码前缀自动推断市场
        code = str(c).strip()
        if len(code) == 6:
            if code.startswith(('600', '601', '603', '605', '688', '110', '113')):
                return 'sh' + code
            elif code.startswith(('000', '001', '002', '003', '004', '300', '127', '128')):
                return 'sz' + code
            elif code.startswith(('830', '831', '832', '833', '834', '835', '836', '837', '838', '839',
                                  '870', '871', '872', '873', '874', '875', '876', '877', '878')):
                return 'bj' + code
        return code



def is_all_holiday(start: Union[str, date], end: Union[str, date]) -> bool:
    """
    判断传入的开始时间和结束时间之间（含首尾）是否全部为周末或中国阳历假期
    
    Args:
        start: 开始日期，支持 'YYYYMMDD' 字符串或 date 对象
        end: 结束日期，支持 'YYYYMMDD' 字符串或 date 对象
        
    Returns:
        若范围内每一天都是周末或阳历假期，返回 True；否则返回 False
    """
    # 中国法定节假日（阳历部分，可扩展）
    CHINA_PUBLIC_HOLIDAYS = {
        # 元旦
        "0101",
        # 春节（农历，这里仅示例，实际需按年更新）
        # 清明
        "0404",
        # 五一
        "0501",
        # 端午（农历，示例）
        # 中秋（农历，示例）
        # 国庆
        "1001", "1002", "1003"
    }

    def _is_weekend(d: date) -> bool:
        """判断是否为周六或周日"""
        return d.weekday() >= 5

    def _is_china_public_holiday(d: date) -> bool:
        """判断是否为阳历法定节假日（MMDD格式）"""
        return d.strftime("%m%d") in CHINA_PUBLIC_HOLIDAYS

    if isinstance(start, str):
        start = datetime.strptime(start, "%Y%m%d").date()
    if isinstance(end, str):
        end = datetime.strptime(end, "%Y%m%d").date()
    
    if start > end:
        return False
    
    cur = start
    while cur <= end:
        if not (_is_weekend(cur) or _is_china_public_holiday(cur)):
            return False
        cur += timedelta(days=1)
    return True
