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


# 模块初始化时的设置
__all__ = [
    'DataFetcher',
    'AkshareFetcher', 
    'DataFetchFactory',
    'DataFetchError',
    'get_stock_basic_info',
    'get_stock_daily'
]