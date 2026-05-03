import logging
from datetime import datetime
from typing import Optional, Any
from datetime import date, timedelta
from typing import Optional, Union
# 导入全局路径工具
from stocks.utils import normalize_path, join_path, safe_join


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
            elif code.startswith(('000', '001', '002', '003', '004', '300', '301','302', '127', '128')):
                return 'sz' + code
            elif code.startswith(('830', '831', '832', '833', '834', '835', '836', '837', '838', '839',
                                  '870', '871', '872', '873', '874', '875', '876', '877', '878','920')):
                return 'bj' + code
        return code



def standardize_stock_code(code: str) -> str:
    """
    标准化股票代码，移除市场前缀（如sh、sz、bj）
    
    Args:
        code: 股票代码，可以是带市场前缀的格式（如sh600000）或纯数字格式（如600000）
        
    Returns:
        标准化后的股票代码（仅数字部分）
    """
    if not code:
        return code
    
    code_str = str(code).strip().lower()
    # 移除常见的市场前缀
    if code_str.startswith(('sh', 'sz', 'bj')):
        # 确保前缀后是数字
        suffix = code_str[2:]
        if suffix.isdigit():
            return suffix
    # 如果已经是纯数字，直接返回
    elif code_str.isdigit():
        return code_str
    
    # 返回原始代码（如果无法标准化）
    return code_str

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


def save_to_csv(code, df, file_name:str=None):
    """
    保存股票数据到CSV文件
    
    Args:
        code: 股票代码
        df: 包含股票数据的DataFrame
        adj: 复权类型（已废弃，仅用于兼容旧代码）
        
    Returns:
        保存的行数，如果失败返回0
    """
    import os
    import csv
    import pandas as pd
    import logging
    
    logger = logging.getLogger(__name__)
    
    if df is None or getattr(df, 'empty', True):
        return 0
    try:
        # 创建CSV保存目录
        if file_name is None:
            csv_dir = join_path(os.path.dirname(__file__), '..', 'data', 'daily')
        else:
            csv_dir = normalize_path(file_name)
            
        os.makedirs(csv_dir, exist_ok=True)
        
        # 重命名列名，与数据库保持一致
        cols = {
            'date': 'date',
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turnover',
            '流通股本': 'outstanding_share'
        }
        df = df.rename(columns=cols)
        
        # 添加股票代码列（如果不存在）
        if 'code' not in df.columns:
            df['code'] = code
        
        # 确保日期格式正确
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')  # 格式化为ISO 8601格式
        
        # 转换数值类型，确保正确的数值格式
        numeric_columns = ['open', 'close', 'high', 'low', 'amount', 'turnover', 'outstanding_share']
        for col in numeric_columns:
            if col in df.columns:
                # 尝试将列转换为数值类型
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 特别处理volume列为LONG型（int64）
        if 'volume' in df.columns:
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')
        
        # 按照数据库列顺序调整DataFrame列顺序
        # 数据库列顺序：code, date, open, close, high, low, volume, amount, turnover, outstanding_share
        db_columns = ['code', 'date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover', 'outstanding_share']
        
        # 只保留存在的列
        available_columns = []
        for col in db_columns:
            if col in df.columns:
                available_columns.append(col)
        
        # 添加其他可能存在的列
        for col in df.columns:
            if col not in available_columns:
                available_columns.append(col)
        
        # 移除adjust_type列（如果存在）
        if 'adjust_type' in df.columns:
            df = df.drop(columns=['adjust_type'])
        
        # 调整列顺序
        df = df[available_columns]
        
        # 构建文件名
        csv_file = join_path(csv_dir, f'{code}.csv')
        
        # 保存到CSV文件，使用QUOTE_ALL确保所有字段都用双引号包围
        # float_format=None让pandas自动选择合适的数值格式，可能包括科学计数法
        df.to_csv(csv_file, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL, float_format=None)
        
        return len(df)
    except Exception as e:
        logger.exception("保存到CSV文件失败: code=%s, error=%s", code, e)
        return 0
