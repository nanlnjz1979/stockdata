#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据抓取接口模块
提供统一的数据抓取接口，支持调用各种版本的数据源接口
"""

import logging
import time
from typing import Dict, List, Tuple, Optional, Union
import pandas as pd
from backend.global_config.utils import make_symbol

logger = logging.getLogger(__name__)


class DataFetchError(Exception):
    """数据抓取异常类"""
    pass


class DataFetcher:
    """数据抓取器基类"""
    
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        """
        初始化数据抓取器
        
        Args:
            max_retries: 最大重试次数
            retry_delay: 重试间隔（秒）
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    def _retry_wrapper(self, func, *args, **kwargs):
        """
        重试包装器，处理接口调用的重试逻辑
        
        Args:
            func: 要调用的函数
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            函数调用结果
            
        Raises:
            DataFetchError: 达到最大重试次数后仍失败
        """
        retries = 0
        last_exception = None
        
        while retries <= self.max_retries:
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                last_exception = e
                retries += 1
                if retries <= self.max_retries:
                    logger.warning(f"调用失败，{retries}/{self.max_retries}次重试中: {str(e)}")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"达到最大重试次数，调用失败: {str(e)}")
        
        raise DataFetchError(f"数据抓取失败: {str(last_exception)}")


class AkshareFetcher(DataFetcher):
    """
    Akshare数据源抓取器
    支持调用不同版本的akshare接口
    """
    
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0, ak=None):
        """
        初始化Akshare抓取器
        
        Args:
            max_retries: 最大重试次数
            retry_delay: 重试间隔（秒）
            ak: 已初始化的akshare实例，如果为None则尝试导入
        """
        super().__init__(max_retries, retry_delay)
        self.ak = ak
        
        # 如果没有提供ak实例，尝试导入
        if self.ak is None:
            try:
                import akshare as ak
                self.ak = ak
                logger.info("成功导入akshare库")
            except ImportError:
                logger.error("无法导入akshare库")
                self.ak = None
    
    def is_available(self) -> bool:
        """
        检查数据源是否可用
        
        Returns:
            bool: 数据源是否可用
        """
        return self.ak is not None
    
    def fetch_stock_basic_info(self, market: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        获取股票基本信息
        
        Args:
            market: 市场代码，可选值：'SH'(上海), 'SZ'(深圳), 'BJ'(北京)，None表示全部市场
            
        Returns:
            Dict[str, pd.DataFrame]: 市场代码到股票信息DataFrame的映射
            
        Raises:
            DataFetchError: 数据抓取失败
        """
        if not self.is_available():
            raise DataFetchError("akshare不可用")
        
        results = {}
        markets_to_fetch = []
        
        if market is None:
            markets_to_fetch = ['SH', 'SZ', 'BJ']
        elif market in ['SH', 'SZ', 'BJ']:
            markets_to_fetch = [market]
        else:
            raise ValueError(f"不支持的市场代码: {market}")
        
        # 上海市场股票信息
        if 'SH' in markets_to_fetch:
            try:
                df = self._retry_wrapper(self.ak.stock_info_sh_name_code)
                results['SH'] = df
                logger.info(f"成功获取上海市场股票信息，共{len(df)}条")
            except Exception as e:
                logger.error(f"获取上海市场股票信息失败: {str(e)}")
        
        # 深圳市场股票信息
        if 'SZ' in markets_to_fetch:
            try:
                df = self._retry_wrapper(self.ak.stock_info_sz_name_code)
                results['SZ'] = df
                logger.info(f"成功获取深圳市场股票信息，共{len(df)}条")
            except Exception as e:
                logger.error(f"获取深圳市场股票信息失败: {str(e)}")
        
        # 北京市场股票信息
        if 'BJ' in markets_to_fetch:
            try:
                df = self._retry_wrapper(self.ak.stock_info_bj_name_code)
                results['BJ'] = df
                logger.info(f"成功获取北京市场股票信息，共{len(df)}条")
            except Exception as e:
                logger.error(f"获取北京市场股票信息失败: {str(e)}")
        
        if not results:
            raise DataFetchError("无法获取任何市场的股票信息")
        
        return results
    
    def fetch_stock_daily(self, code: str, start_date: str, end_date: str, adjust: str = "") -> pd.DataFrame:
        """
        获取股票日K线数据
        
        Args:
            code: 股票代码
            start_date: 开始日期，格式如'2023-01-01'
            end_date: 结束日期，格式如'2023-12-31'
            adjust: 复权类型，可选值：'qfq'(前复权), 'hfq'(后复权), None(不复权)
            
        Returns:
            pd.DataFrame: 股票日K线数据
            
        Raises:
            DataFetchError: 数据抓取失败
        """
        if not self.is_available():
            raise DataFetchError("akshare不可用")
        
        try:

            # 首先尝试调用 stock_zh_a_daily
            try:
                logger.debug(f"尝试使用 stock_zh_a_daily 获取股票{code}数据")
                df = self._retry_wrapper(
                    self.ak.stock_zh_a_daily,
                    symbol=make_symbol(code),
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust
                )
                logger.info(f"成功使用 stock_zh_a_daily 获取股票{code}的日K线数据，时间范围：{start_date}至{end_date}")
                return df
            except Exception as daily_error:
                logger.warning(f"使用 stock_zh_a_daily 获取股票{code}数据失败，尝试降级到 stock_zh_a_hist: {str(daily_error)}")
                
                # 降级调用 stock_zh_a_hist
                df = self._retry_wrapper(
                    self.ak.stock_zh_a_hist,
                    symbol=code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust
                )
                logger.info(f"成功使用 stock_zh_a_hist 获取股票{code}的日K线数据，时间范围：{start_date}至{end_date}")
                return df
        except Exception as e:
            raise DataFetchError(f"获取股票{code}日K线数据失败: {str(e)}")
    
    def fetch_sw_industry_first_info(self) -> pd.DataFrame:
        """
        获取申万一级行业数据
        
        Returns:
            pd.DataFrame: 申万一级行业数据
            
        Raises:
            DataFetchError: 数据抓取失败
        """
        if not self.is_available():
            raise DataFetchError("akshare不可用")
        
        try:
            logger.info("开始抓取申万一级行业数据")
            df = self._retry_wrapper(self.ak.sw_index_first_info)
            logger.info(f"成功抓取申万一级行业数据，共{len(df)}条")
            return df
        except Exception as e:
            raise DataFetchError(f"获取申万一级行业数据失败: {str(e)}")
    
    def fetch_sw_industry_second_info(self) -> pd.DataFrame:
        """
        获取申万二级行业数据
        
        Returns:
            pd.DataFrame: 申万二级行业数据
            
        Raises:
            DataFetchError: 数据抓取失败
        """
        if not self.is_available():
            raise DataFetchError("akshare不可用")
        
        try:
            logger.info("开始抓取申万二级行业数据")
            df = self._retry_wrapper(self.ak.sw_index_second_info)
            logger.info(f"成功抓取申万二级行业数据，共{len(df)}条")
            return df
        except Exception as e:
            raise DataFetchError(f"获取申万二级行业数据失败: {str(e)}")
    
    def fetch_sw_industry_third_info(self) -> pd.DataFrame:
        """
        获取申万三级行业数据
        
        Returns:
            pd.DataFrame: 申万三级行业数据
            
        Raises:
            DataFetchError: 数据抓取失败
        """
        if not self.is_available():
            raise DataFetchError("akshare不可用")
        
        try:
            logger.info("开始抓取申万三级行业数据")
            df = self._retry_wrapper(self.ak.sw_index_third_info)
            logger.info(f"成功抓取申万三级行业数据，共{len(df)}条")
            return df
        except Exception as e:
            raise DataFetchError(f"获取申万三级行业数据失败: {str(e)}")


class DataFetchFactory:
    """
    数据抓取工厂类
    用于创建不同类型的数据抓取器
    """
    
    _fetchers = {}
    
    @classmethod
    def get_fetcher(cls, fetcher_type: str = "akshare", **kwargs) -> DataFetcher:
        """
        获取数据抓取器实例
        
        Args:
            fetcher_type: 抓取器类型，目前支持'akshare'
            **kwargs: 传递给抓取器构造函数的参数
            
        Returns:
            DataFetcher: 数据抓取器实例
            
        Raises:
            ValueError: 不支持的抓取器类型
        """
        # 使用缓存避免重复创建实例
        key = (fetcher_type, str(sorted(kwargs.items())))
        
        if key not in cls._fetchers:
            if fetcher_type == "akshare":
                cls._fetchers[key] = AkshareFetcher(**kwargs)
            else:
                raise ValueError(f"不支持的数据抓取器类型: {fetcher_type}")
        
        return cls._fetchers[key]


# 导出常用方法，提供更简洁的调用方式
def get_stock_basic_info(market: Optional[str] = None, **kwargs) -> Dict[str, pd.DataFrame]:
    """
    获取股票基本信息的便捷方法
    """
    fetcher = DataFetchFactory.get_fetcher(**kwargs)
    return fetcher.fetch_stock_basic_info(market)


def get_stock_daily(code: str, start_date: str, end_date: str, adjust: str = "qfq", **kwargs) -> pd.DataFrame:
    """
    获取股票日K线数据的便捷方法
    """
    fetcher = DataFetchFactory.get_fetcher(**kwargs)
    return fetcher.fetch_stock_daily(code, start_date, end_date, adjust)
    
from datetime import datetime, date as date_type
from typing import Union
from backend.global_config.file_config import FileConfig

def is_trading_day(date: Union[str, datetime, date_type]) -> bool:
    """
    检查指定日期是否为交易日
    
    Args:
        date: 待检查的日期，格式为'YYYY-MM-DD'或datetime对象
        
    Returns:
        bool: 是否为交易日
        
    Raises:
        DataFetchError: 数据获取失败
    """
    if isinstance(date, datetime):
        date_str = date.strftime("%Y-%m-%d")
        date_obj = date.date()
    elif isinstance(date, date_type):
        date_str = date.strftime("%Y-%m-%d")
        date_obj = date
    else:
        date_str = date
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            raise DataFetchError(f"无效的日期格式: {date_str}")
    
    # 首先检查是否为周末
    if date_obj.weekday() >= 5:  # 0=周一, 4=周五, 5=周六, 6=周日
        return False
    
    # 先从FileConfig获取交易日历
    trading_dates = FileConfig.get('trading_dates', None)
    
    if trading_dates is not None:
        # 如果配置中有交易日历，直接使用
        logger.debug(f"从配置获取交易日历，检查日期: {date_str}")
        return date_str in trading_dates
    
    # 如果配置中没有交易日历，使用akshare获取
    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        if df is None or df.empty:
            raise DataFetchError("akshare返回空数据")

        # 假设返回的DataFrame中日期列名为'trade_date'，格式为'YYYY-MM-DD'
        trading_dates = set(df.iloc[:, 0].astype(str).tolist())
        
        # 将获取到的交易日历保存到FileConfig中
        FileConfig.set('trading_dates', list(trading_dates))
        logger.info(f"已将交易日历保存到配置中")
        
        logger.debug(f"通过akshare获取交易日历，检查日期: {date_str}")
        return date_str in trading_dates
    except ImportError:
        logger.warning("akshare库未安装，仅检查是否为工作日")
        # 如果akshare不可用，仅返回是否为工作日（周一至周五）
        return date_obj.weekday() < 5
    except Exception as e:
        logger.error(f"通过akshare获取交易日历失败: {str(e)}")
        # 出错时回退到工作日检查
        return date_obj.weekday() < 5

def get_sw_industry_first_info(**kwargs) -> pd.DataFrame:
    """
    获取申万一级行业数据的便捷方法
    """
    fetcher = DataFetchFactory.get_fetcher(**kwargs)
    return fetcher.fetch_sw_industry_first_info()

def get_sw_industry_second_info(**kwargs) -> pd.DataFrame:
    """
    获取申万二级行业数据的便捷方法
    """
    fetcher = DataFetchFactory.get_fetcher(**kwargs)
    return fetcher.fetch_sw_industry_second_info()

def get_sw_industry_third_info(**kwargs) -> pd.DataFrame:
    """
    获取申万三级行业数据的便捷方法
    """
    fetcher = DataFetchFactory.get_fetcher(**kwargs)
    return fetcher.fetch_sw_industry_third_info()

# 模块初始化时的设置
__all__ = [
    'DataFetcher',
    'AkshareFetcher', 
    'DataFetchFactory',
    'DataFetchError',
    'get_stock_basic_info',
    'get_stock_daily',
    'get_sw_industry_first_info',
    'get_sw_industry_second_info',
    'get_sw_industry_third_info',
    'is_trading_day'
]