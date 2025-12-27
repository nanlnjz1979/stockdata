#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据抓取接口模块
提供统一的数据抓取接口，支持调用各种版本的数据源接口
"""

import logging
import time
from datetime import datetime
from typing import Dict, List,  Optional, Union, Any
import pandas as pd
from backend.global_config.utils import make_symbol
import akshare as ak
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
    
    def get_all_indices(self) -> List[Dict[str, Any]]:
        """
        获取所有指数列表
        
        Returns:
            List[Dict[str, Any]]: 指数列表，包含指数代码和名称等信息
        """
        if not self.is_available():
            logger.error("akshare不可用，无法获取指数列表")
            return []
        
        try:
            logger.info("开始获取所有指数列表")
            
            # 调用akshare的index_stock_info函数获取所有指数信息
            df = self._retry_wrapper(self.ak.index_stock_info)
            
            if df is None:
                logger.warning("获取指数数据返回None")
                return []
            
            logger.info(f"获取到指数数据，数据类型: {type(df)}")
            logger.info(f"数据形状: {df.shape}")
            logger.info(f"列名: {list(df.columns)}")
            
            if df.empty:
                logger.warning("获取到的指数数据为空")
                return []
            
            # 打印前几行数据，便于调试
            logger.info(f"数据前5行:\n{df.head()}")
            
            # 处理获取到的数据
            indices = []
            for _, r in df.iterrows():
                # 尝试从不同的列名中获取指数代码和名称
                code = None
                name = None
                
                # 打印每一行的所有列和值，便于调试
                logger.debug(f"行数据: {r.to_dict()}")
                
                # 获取指数代码
                for key in ['代码', '证券代码', '指数代码', 'symbol', 'code', 'index_code']:
                    if key in r:
                        v = r[key]
                        if v:
                            code = str(v).strip()
                            logger.debug(f"从列'{key}'获取到代码: {code}")
                            break
                
                # 获取指数名称
                for key in ['名称', '证券简称', '指数名称', 'name', 'index_name','display_name']:
                    if key in r:
                        v = r[key]
                        if v:
                            name = str(v).strip()
                            logger.debug(f"从列'{key}'获取到名称: {name}")
                            break
                
                if code:
                    indices.append({
                        'code': code,
                        'name': name or code
                    })
                else:
                    logger.warning(f"无法从行数据中获取指数代码: {r.to_dict()}")
            
            logger.info(f"成功获取所有指数列表，共{len(indices)}个指数")
            return indices
            
        except Exception as e:
            logger.error(f"获取所有指数列表失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def fetch_index_stock_cons(self, symbol: str = "000300") -> List[Dict[str, Any]]:
        """
        获取指数成分股
        
        Args:
            symbol: 指数代码，默认为"000300"（沪深300指数）
            
        Returns:
            List[Dict[str, Any]]: 指数成分股列表，包含股票代码和名称等信息
        """
        if not self.is_available():
            logger.error("akshare不可用，无法获取指数成分股")
            return []
        
        try:
            logger.info(f"开始获取指数{symbol}的成分股")
            
            # 优先使用index_stock_cons_csindex获取指数成分股，失败后回退到index_stock_cons
            try:
                logger.info(f"尝试使用index_stock_cons_csindex获取指数{symbol}的成分股")
                df = self._retry_wrapper(self.ak.index_stock_cons_csindex, symbol=symbol)
            except Exception as e:
                logger.warning(f"使用index_stock_cons_csindex获取指数{symbol}的成分股失败: {str(e)}")
                logger.info(f"回退到使用index_stock_cons获取指数{symbol}的成分股")
                # 回退到使用index_stock_cons
                df = self._retry_wrapper(self.ak.index_stock_cons, symbol=symbol)
            
            if df is None or df.empty:
                logger.warning(f"未获取到指数{symbol}的有效成分股数据")
                return []
            
            # 处理获取到的数据
            index_stocks = []
            for _, r in df.iterrows():
                # 尝试从不同的列名中获取股票代码和名称
                code = None
                name = None
                
                # 获取股票代码
                for key in ['代码', '证券代码', 'A股代码', '股票代码', '成分券代码', 'symbol', 'code','品种代码']:
                    v = r.get(key)
                    if v:
                        code = str(v).strip()
                        break
                
                # 获取股票名称
                for key in ['名称', '证券简称', 'A股简称', '股票简称', '成分券名称', 'name','品种名称']:
                    v = r.get(key)
                    if v:
                        name = str(v).strip()
                        break
                
                if code:
                    index_stocks.append({
                        'code': code,
                        'name': name or code
                    })
            
            logger.info(f"成功获取指数{symbol}的成分股，共{len(index_stocks)}只")
            return index_stocks
            
        except Exception as e:
            logger.error(f"获取指数{symbol}的成分股失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def fetch_all_stock_basic_info(self) -> List[Dict[str, Any]]:
        """
        获取所有市场（SH/SZ/BJ）的股票基础信息
        
        Returns:
            List[Dict[str, Any]]: 股票基础信息列表
        """
        if not self.is_available():
            logger.error("akshare不可用，无法获取股票基础信息")
            return []
        
        stock_info_list = []
        dfs = []
        
        try:
            # 获取上海市场股票信息
            try:
                dfs.append(('SH', self._retry_wrapper(self.ak.stock_info_sh_name_code)))
                logger.info("成功获取上海市场股票信息")
            except Exception as e:
                logger.error(f"获取上海市场股票信息失败: {str(e)}")
            
            # 获取深圳市场股票信息
            try:
                dfs.append(('SZ', self._retry_wrapper(self.ak.stock_info_sz_name_code)))
                logger.info("成功获取深圳市场股票信息")
            except Exception as e:
                logger.error(f"获取深圳市场股票信息失败: {str(e)}")
            
            # 获取北京市场股票信息
            try:
                dfs.append(('BJ', self._retry_wrapper(self.ak.stock_info_bj_name_code)))
                logger.info("成功获取北京市场股票信息")
            except Exception as e:
                logger.error(f"获取北京市场股票信息失败: {str(e)}")
            
            # 处理获取到的数据
            for market, df in dfs:
                if df is None or df.empty:
                    continue
                
                for _, r in df.iterrows():
                    code = None
                    # 尝试从不同的列名中获取股票代码
                    for key in ['代码', '证券代码', 'A股代码', '股票代码']:
                        v = r.get(key)
                        if v:
                            code = str(v).strip()
                            break
                    
                    if not code:
                        continue
                    
                    name = None
                    # 尝试从不同的列名中获取股票名称
                    for key in ['证券简称', 'A股简称', '股票简称']:
                        v = r.get(key)
                        if v:
                            name = str(v).strip()
                            break
                    
                    company_name = None
                    # 尝试从不同的列名中获取公司名称
                    for key in ['公司名称', '公司全称', '企业名称', '证券简称', 'A股简称']:
                        v = r.get(key)
                        if v:
                            company_name = str(v).strip()
                            break
                    
                    listing_date = None
                    # 尝试从不同的列名中获取上市日期
                    for key in ['上市日期', '上市时间', 'A股上市日期']:
                        v = r.get(key)
                        if v:
                            try:
                                listing_date = str(v).strip()
                               
                            except Exception as e:
                                logger.warning(f"解析上市日期失败 {v}: {str(e)}")
                                listing_date = None
                            break
                    
                    # 构建股票信息字典
                    stock_info = {
                        'code': code,
                        'name': name or code,
                        'company_name': company_name or name or code,
                        'market': market,
                        'listing_date': str(listing_date)
                    }
                    stock_info_list.append(stock_info)
                    
            logger.info(f"共获取到 {len(stock_info_list)} 条股票基础信息")
            return stock_info_list
            
        except Exception as e:
            logger.error(f"获取股票基础信息时发生错误: {str(e)}")
            return []
     
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
                """
                df = ak.stock_zh_a_daily(
                    symbol=make_symbol(code),
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust
                )
                """
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
    
    def fetch_stock_adjust_factor(self, code: str, adjust: str = "qfq-factor") -> pd.DataFrame:
        """
        获取股票复权因子数据
        
        Args:
            code: 股票代码
            adjust: 复权因子类型，可选值：'qfq-factor'(前复权因子), 'hfq-factor'(后复权因子), 'bfq-factor'(不复权因子)
            
        Returns:
            pd.DataFrame: 复权因子数据，包含日期、收盘价、复权因子等字段
            
        Raises:
            DataFetchError: 数据抓取失败
        """
        if not self.is_available():
            raise DataFetchError("akshare不可用")
        
        try:
            logger.debug(f"尝试获取股票{code}的复权因子数据，复权类型：{adjust}")
            
            # 使用ak.stock_zh_a_daily获取复权因子数据，不需要指定开始时间和结束时间
            df = self._retry_wrapper(
                self.ak.stock_zh_a_daily,
                symbol=make_symbol(code),
                adjust=adjust
            )
            
            logger.info(f"成功获取股票{code}的复权因子数据")
            return df
        except Exception as e:
            logger.error(f"获取股票{code}的复权因子数据失败: {str(e)}")
            raise DataFetchError(f"获取复权因子数据失败: {str(e)}")
    
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
    
    def fetch_sw_index_third_cons(self, symbol: Union[str, List[str]]) -> pd.DataFrame:
        """
        获取申万三级行业成分股
        
        Args:
            symbol: 行业代码，可以是单个字符串或字符串列表，格式如"850111.SI"
            
        Returns:
            pd.DataFrame: 行业成分股数据
            
        Raises:
            DataFetchError: 数据抓取失败
        """
        if not self.is_available():
            raise DataFetchError("akshare不可用")
        
        try:
            logger.info(f"开始抓取申万三级行业成分股数据，行业代码: {symbol}")
            df = self._retry_wrapper(self.ak.sw_index_third_cons, symbol=symbol)
            logger.info(f"成功抓取申万三级行业成分股数据，共{len(df)}条")
            return df
        except Exception as e:
            raise DataFetchError(f"获取申万三级行业成分股数据失败: {str(e)}")
    
    def fetch_sw_industry_data(self, industry_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取申万行业指标数据
        
        Args:
            industry_code: 申万行业指标代码
            start_date: 开始日期，格式如'2023-01-01'
            end_date: 结束日期，格式如'2023-12-31'
            
        Returns:
            pd.DataFrame: 申万行业指标数据
            
        Raises:
            DataFetchError: 数据抓取失败
        """
        if not self.is_available():
            raise DataFetchError("akshare不可用")
        
        try:
            logger.info(f"开始抓取申万行业指标数据，代码: {industry_code}，时间范围: {start_date}至{end_date}")
            df = self._retry_wrapper(
                self.ak.sw_index_daily,
                symbol=industry_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"成功抓取申万行业指标数据，共{len(df)}条")
            return df
        except Exception as e:
            raise DataFetchError(f"获取申万行业指标数据失败: {str(e)}")
    
    def fetch_stock_financial_data(self, code: str) -> pd.DataFrame:
        """
        获取股票财务数据并处理
        
        Args:
            code: 股票代码
            
        Returns:
            pd.DataFrame: 处理后的财务数据
            
        Raises:
            DataFetchError: 数据抓取失败
        """
        if not self.is_available():
            raise DataFetchError("akshare不可用")
        
        try:
            logger.info(f"开始抓取股票{code}的财务摘要数据")
            # 调用ak.stock_financial_abstract获取财务数据
            df = self._retry_wrapper(
                self.ak.stock_financial_abstract,
                symbol=code
            )
            logger.info(f"成功抓取股票{code}的财务摘要数据")
            
            # 处理数据
            processed_df = _get_and_process_financial_data(df, code)
            
            return processed_df
        except Exception as e:
            raise DataFetchError(f"获取股票{code}财务数据失败: {str(e)}")
    
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



def get_stock_daily(code: str, start_date: str, end_date: str, adjust: str = "", **kwargs) -> pd.DataFrame:
    """
    获取股票日K线数据的便捷方法
    """
    fetcher = DataFetchFactory.get_fetcher(**kwargs)
    return fetcher.fetch_stock_daily(code, start_date, end_date, adjust)
    
# 中文指标到英文缩写的硬编码映射字典
# 格式："大分类_中文指标" -> "大分类缩写_英文指标缩写"（下划线替换连字符）
CHINESE_TO_ENGLISH_MAPPING = {
    # 常用指标 (Common Metrics - CM)
    "常用指标_归母净利润": "CM_NPAS",
    "常用指标_营业总收入": "CM_TOR",
    "常用指标_营业成本": "CM_OC",
    "常用指标_净利润": "CM_NP",
    "常用指标_扣非净利润": "CM_NRNP",
    "常用指标_股东权益合计(净资产)": "CM_TSE_NA",
    "常用指标_商誉": "CM_GW",
    "常用指标_经营现金流量净额": "CM_NOCF",
    "常用指标_基本每股收益": "CM_BEPS",
    "常用指标_每股净资产": "CM_NAPS",
    "常用指标_每股现金流": "CM_CFPS",
    "常用指标_净资产收益率(ROE)": "CM_ROE",
    "常用指标_总资产报酬率(ROA)": "CM_ROA",
    "常用指标_毛利率": "CM_GM",
    "常用指标_销售净利率": "CM_NPM",
    "常用指标_期间费用率": "CM_PER",
    "常用指标_资产负债率": "CM_ALR",
    
    # 每股指标 (Per Share Indicators - PSI)
    "每股指标_基本每股收益": "PSI_BEPS",
    "每股指标_稀释每股收益": "PSI_DEPS",
    "每股指标_摊薄每股收益_最新股数": "PSI_DEPS_LSC",
    "每股指标_摊薄每股净资产_期末股数": "PSI_DNAPS_PSC",
    "每股指标_调整每股净资产_期末股数": "PSI_ANAPS_PSC",
    "每股指标_每股净资产_最新股数": "PSI_NAPS_LSC",
    "每股指标_每股经营现金流": "PSI_OCFPS",
    "每股指标_每股现金流量净额": "PSI_NCFPS",
    "每股指标_每股企业自由现金流量": "PSI_FCFFPS",
    "每股指标_每股股东自由现金流量": "PSI_FCFEPS",
    "每股指标_每股未分配利润": "PSI_UPPS",
    "每股指标_每股资本公积金": "PSI_CRPS",
    "每股指标_每股盈余公积金": "PSI_SRPS",
    "每股指标_每股留存收益": "PSI_REPS",
    "每股指标_每股营业收入": "PSI_ORPS",
    "每股指标_每股营业总收入": "PSI_TORPS",
    "每股指标_每股息税前利润": "PSI_EBITPS",
    
    # 盈利能力 (Profitability - PCP)
    "盈利能力_净资产收益率(ROE)": "PCP_ROE",
    "盈利能力_摊薄净资产收益率": "PCP_DROE",
    "盈利能力_净资产收益率_平均": "PCP_AROE",
    "盈利能力_净资产收益率_平均_扣除非经常损益": "PCP_AROE_ENR",
    "盈利能力_摊薄净资产收益率_扣除非经常损益": "PCP_DROE_ENR",
    "盈利能力_息税前利润率": "PCP_EBITM",
    "盈利能力_总资产报酬率": "PCP_ROA",
    "盈利能力_总资本回报率": "PCP_ROTC",
    "盈利能力_投入资本回报率": "PCP_ROIC",
    "盈利能力_息前税后总资产报酬率_平均": "PCP_AROAAt_EI",
    "盈利能力_毛利率": "PCP_GM",
    "盈利能力_销售净利率": "PCP_NPM",
    "盈利能力_成本费用利润率": "PCP_CEPR",
    "盈利能力_营业利润率": "PCP_OPM",
    "盈利能力_总资产净利率_平均": "PCP_ANPMTA",
    "盈利能力_总资产净利率_平均(含少数股东损益)": "PCP_ANPMTA_IMI",
    
    # 成长能力 (Growth Capability - GCP)
    "成长能力_归母净利润": "GCP_NPAS",
    "成长能力_营业总收入": "GCP_TOR",
    "成长能力_净利润": "GCP_NP",
    "成长能力_扣非净利润": "GCP_NRNP",
    "成长能力_营业总收入增长率": "GCP_TORGR",
    "成长能力_归属母公司净利润增长率": "GCP_GRNPAPC",
    
    # 收益质量 (Earnings Quality - EQL)
    "收益质量_经营活动净现金/销售收入": "EQL_NOCF_SR",
    "收益质量_经营性现金净流量/营业总收入": "EQL_NOCF_TOR",
    "收益质量_成本费用率": "EQL_CER",
    "收益质量_期间费用率": "EQL_PER",
    "收益质量_销售成本率": "EQL_CSR",
    "收益质量_经营活动净现金/归属母公司的净利润": "EQL_NOCF_NPAPC",
    "收益质量_所得税/利润总额": "EQL_IT_TP",
    
    # 财务风险 (Financial Risk - FR)
    "财务风险_流动比率": "FR_CR",
    "财务风险_速动比率": "FR_QR",
    "财务风险_保守速动比率": "FR_CQR",
    "财务风险_资产负债率": "FR_ALR",
    "财务风险_权益乘数": "FR_EM",
    "财务风险_权益乘数(含少数股权的净资产)": "FR_EM_IMINA",
    "财务风险_产权比率": "FR_DER",
    "财务风险_现金比率": "FR_CashR",
    
    # 营运能力 (Operating Capability - OCP)
    "营运能力_应收账款周转率": "OCP_ART",
    "营运能力_应收账款周转天数": "OCP_ARTD",
    "营运能力_存货周转率": "OCP_IT",
    "营运能力_存货周转天数": "OCP_ITD",
    "营运能力_总资产周转率": "OCP_TAT",
    "营运能力_总资产周转天数": "OCP_TATD",
    "营运能力_流动资产周转率": "OCP_CAT",
    "营运能力_流动资产周转天数": "OCP_CATD",
    "营运能力_应付账款周转率": "OCP_APT"
}

def _get_and_process_financial_data(stock_financial_abstract_df,
                                   stock_symbol):
    """
    处理股票财务数据
    
    参数:
    stock_financial_abstract_df: pd.DataFrame, 股票财务摘要数据
    stock_symbol: str, 股票代码
    
    返回:
    pd.DataFrame, 处理后的财务数据，格式为：股票代码在前，日期在后，指标最后
    """
    # 打印获取的数据
    print(stock_financial_abstract_df)
    
    # 打印映射字典示例
    print("\n--- 中文到英文指标映射示例 ---")
    for chinese_key, english_abbr in list(CHINESE_TO_ENGLISH_MAPPING.items())[:5]:
        print(f"{chinese_key} -> {english_abbr}")
    
    # 1. 提取指标信息（大分类、指标名称、英文缩写）
    indicator_info = stock_financial_abstract_df[['选项', '指标']].copy()
    indicator_info['英文缩写'] = indicator_info.apply(lambda row: CHINESE_TO_ENGLISH_MAPPING.get(f"{row['选项']}_{row['指标']}", "UNKNOWN"), axis=1)
    
    # 2. 提取日期列（从第三列开始都是日期）
    date_columns = stock_financial_abstract_df.columns[2:].tolist()
    
    # 3. 创建按日期组织的新DataFrame
    # 第一列：股票代码
    # 第二列：日期（格式为YYYY-MM-DD）
    # 后续列：每个指标的数值，列名为指标的英文缩写
    new_data = []
    for date in date_columns:
        # 将日期从YYYYMMDD格式转换为YYYY-MM-DD格式
        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        # 为每个日期创建一行数据，股票代码作为第一列
        row_data = {'code': stock_symbol, 'date': formatted_date}
        for idx, row in indicator_info.iterrows():
            # 获取该日期下对应指标的数值
            row_data[row['英文缩写']] = stock_financial_abstract_df.loc[idx, date]
        new_data.append(row_data)
    
    # 4. 创建最终DataFrame
    final_df = pd.DataFrame(new_data)
    
    return final_df
    
from datetime import datetime, date as date_type
from typing import Union
from backend.global_config.file_config import DataConfig

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
    trading_dates = DataConfig.get('trading_dates', None)
    
    if trading_dates is not None:
        # 如果配置中有交易日历，直接使用
        logger.debug(f"从配置获取交易日历，检查日期: {date_str}")
        return date_str in trading_dates
    
    # 如果配置中没有交易日历，使用akshare获取
    try:
        
        df = ak.tool_trade_date_hist_sina()
        if df is None or df.empty:
            raise DataFetchError("akshare返回空数据")

        # 假设返回的DataFrame中日期列名为'trade_date'，格式为'YYYY-MM-DD'
        trading_dates = set(df.iloc[:, 0].astype(str).tolist())
        
        # 将获取到的交易日历保存到FileConfig中
        DataConfig.set('trading_dates', list(trading_dates))
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

def get_sw_index_third_cons(symbol: Union[str, List[str]], **kwargs) -> pd.DataFrame:
    """
    获取申万三级行业成分股的便捷方法
    
    Args:
        symbol: 行业代码，可以是单个字符串或字符串列表，格式如"850111.SI"
        **kwargs: 传递给抓取器的参数
        
    Returns:
        pd.DataFrame: 行业成分股数据
    """
    fetcher = DataFetchFactory.get_fetcher(**kwargs)
    return fetcher.fetch_sw_index_third_cons(symbol)

def get_sw_industry_data(industry_code: str, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
    """
    获取申万行业指标数据的便捷方法
    
    Args:
        industry_code: 申万行业指标代码
        start_date: 开始日期，格式如'2023-01-01'
        end_date: 结束日期，格式如'2023-12-31'
        **kwargs: 传递给抓取器的参数
        
    Returns:
        pd.DataFrame: 申万行业指标数据
    """
    fetcher = DataFetchFactory.get_fetcher(**kwargs)
    return fetcher.fetch_sw_industry_data(industry_code, start_date, end_date)




# 模块初始化时的设置
__all__ = [
    'DataFetcher',
    'AkshareFetcher', 
    'DataFetchFactory',
    'DataFetchError',
    'get_stock_daily',
    'get_sw_industry_first_info',
    'get_sw_industry_second_info',
    'get_sw_industry_third_info',
    'get_sw_index_third_cons',
    'get_sw_industry_data',
    'is_trading_day'
]