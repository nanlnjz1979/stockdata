import pandas as pd
import os
from typing import Dict, List, Any, Optional
from global_config.utils import standardize_stock_code

class StockData:
    """
    股票数据读取类，用于从CSV文件中读取股票数据
    全局类设计，可以直接通过StockData.GetData调用
    """
    # 类级别的数据目录属性
    _data_dir = os.path.join(os.path.dirname(__file__), '../data/daily')
    # 类级别的股票数据缓存字典，键为股票代码，值为对应的DataFrame
    _stock_dfs: Dict[str, pd.DataFrame] = {}
    
    @classmethod
    def set_data_dir(cls, data_dir: str) -> None:
        """
        设置数据目录路径
        
        Args:
            data_dir: CSV文件存储目录路径
        """
        cls._data_dir = data_dir
    
    @classmethod
    def GetData(cls, code: str, s_time: str, e_time: str, adjust: str) -> pd.DataFrame:
        """
        获取指定股票代码在指定时间范围内的数据
        可以直接通过类名调用：StockData.GetData(...)
        
        Args:
            code: 股票代码（支持带市场前缀如sh、sz、bj或纯数字格式）
            s_time: 开始时间，格式为'YYYY-MM-DD'
            e_time: 结束时间，格式为'YYYY-MM-DD'
            adjust: 复权类型，可以是'前复权'、'后复权'或'不复权'
            
        Returns:
            pd.DataFrame: 包含筛选后股票数据的DataFrame对象
        """
        # 标准化股票代码，移除市场前缀
        std_code = standardize_stock_code(code)
        
        # 检查股票数据是否已经在缓存中
        if std_code not in cls._stock_dfs:
            # 如果不在缓存中，从CSV文件读取
            csv_file = os.path.join(cls._data_dir, f"{std_code}.csv")
            if not os.path.exists(csv_file):
                # 如果文件不存在，返回空的DataFrame
                return pd.DataFrame()
            
            # 定义列的数据类型映射
            dtype_map = {
                'code': str,
                'open': float,
                'close': float,
                'high': float,
                'low': float,
                'volume': int,
                'amount': float,
                'turnover': float,
                'outstanding_share': float
            }
            
            # 读取CSV文件数据到DataFrame，并指定列的数据类型
            df = pd.read_csv(
                csv_file,
                dtype=dtype_map,
                parse_dates=['date'],  # 自动解析date列为日期时间类型
                keep_default_na=False  # 保留原始空字符串，不转换为NaN
            )
            
            # 设置date为索引并存入缓存
            cls._stock_dfs[std_code] = df.set_index('date')
        
        # 从缓存中获取DataFrame
        df = cls._stock_dfs[std_code].copy()
        
        # 打印df的前5行
        #print(df.head())
        # 由于已经设置了date为索引，使用索引进行筛选
        # 根据s_time和e_time筛选时间范围内的数据
        start_datetime = pd.to_datetime(s_time)
        end_datetime = pd.to_datetime(e_time)
        
        # 确保DataFrame不为空且有索引
        if not df.empty and isinstance(df.index, pd.DatetimeIndex):
            # 确保索引已排序
            if not df.index.is_monotonic_increasing:
                df = df.sort_index()
            
            # 处理时区问题：确保索引和查询时间的时区一致性
            # 移除索引的时区信息（如果有时区）
            if df.index.tz is not None:
                df = df.tz_localize(None)
            
            # 使用索引切片进行时间范围筛选
            try:
                df = df.loc[start_datetime:end_datetime]
            except Exception as e:
                # 如果索引切片失败，回退到条件筛选
                print(f"索引切片失败: {e}，尝试条件筛选")
                # 获取索引为日期的DataFrame副本
                df_copy = df.reset_index()
                # 确保date列的时区一致性
                if pd.api.types.is_datetime64tz_dtype(df_copy['date']):
                    df_copy['date'] = df_copy['date'].dt.tz_localize(None)
                # 使用条件筛选
                mask = (df_copy['date'] >= start_datetime) & (df_copy['date'] <= end_datetime)
                df = df_copy[mask].set_index('date')
        else:
            # 如果索引不是DatetimeIndex或DataFrame为空，返回空DataFrame
            df = pd.DataFrame()
        print(df.head())
        # 移除adjust_type相关筛选，因为数据库已移除该字段
        
        # 确保返回的DataFrame中包含date列（将索引转换为列）
        if not df.empty and df.index.name == 'date':
            df = df.reset_index()
        
        return df
    
    # 保留初始化方法以保持向后兼容
    def __init__(self, data_dir: Optional[str] = None):
        """
        初始化StockData类（向后兼容）
        
        Args:
            data_dir: CSV文件存储目录路径，如果不提供则使用默认路径
        """
        if data_dir:
            self.__class__.set_data_dir(data_dir)