#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据抓取接口模块
提供统一的数据抓取接口，支持调用各种版本的数据源接口
"""

import logging
import random
import re
import threading
import time
from datetime import datetime
from typing import Dict, List,  Optional, Union, Any
import pandas as pd
from backend.global_config.utils import make_symbol
import akshare as ak
logger = logging.getLogger(__name__)
_REQUESTS_TIMEOUT_PATCH_LOCK = threading.Lock()
_SOURCE_LOG_NAMES = {
    "stock_zh_a_daily": "daily",
    "stock_zh_a_hist": "hist",
}
_SOURCE_FIRST_WEIGHTS = {
    "stock_zh_a_daily": 8,
    "stock_zh_a_hist": 2,
}


class DataFetchError(Exception):
    """数据抓取异常类"""
    pass


def format_fetch_error(exc: Exception, max_length: int = 240) -> str:
    """把第三方请求异常压缩成适合业务日志的一行摘要。"""
    text = str(exc or "").strip()
    if " error=" in text:
        text = text.rsplit(" error=", 1)[-1].strip()
    for prefix in ("数据抓取失败: ", "获取股票"):
        if prefix != "获取股票" and text.startswith(prefix):
            text = text[len(prefix):].strip()

    if "ProxyError" in text:
        host_match = re.search(r"host='([^']+)'", text)
        host = host_match.group(1) if host_match else "远端接口"
        reason = "代理连接失败"
        if "Cannot connect to proxy" in text:
            reason += ": Cannot connect to proxy"
        if "Remote end closed connection without response" in text:
            reason += "，远端无响应"
        return f"{host} {reason}"

    if "No value to decode" in text:
        return "接口返回空内容或非JSON: No value to decode"

    text = re.sub(r"(https?://[^?\s)]+)\?[^\s)]+", r"\1?...", text)
    text = re.sub(r"(url: /[^?\s)]+)\?[^\s)]+", r"\1?...", text)
    text = re.sub(r"\s+", " ", text)
    if len(text) > max_length:
        return text[:max_length - 3] + "..."
    return text


def format_source_name(source_name: str) -> str:
    return _SOURCE_LOG_NAMES.get(source_name, source_name)


def weighted_source_order(sources):
    """Build a weighted random order so slow sources are less likely to be first."""
    remaining = list(sources)
    ordered = []
    while remaining:
        weights = [_SOURCE_FIRST_WEIGHTS.get(source_name, 1) for source_name, _, _ in remaining]
        selected = random.choices(range(len(remaining)), weights=weights, k=1)[0]
        ordered.append(remaining.pop(selected))
    return ordered


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
    
    def _retry_wrapper(self, func, *args, _retry_context: str = "", _log_final: bool = True, **kwargs):
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
                    logger.debug(
                        "%s调用失败，%d/%d次重试: %s",
                        f"{_retry_context} " if _retry_context else "",
                        retries,
                        self.max_retries,
                        format_fetch_error(e, max_length=180),
                    )
                    time.sleep(self.retry_delay)
                else:
                    if _log_final:
                        logger.warning(
                            "%s达到最大重试次数: %s",
                            f"{_retry_context} " if _retry_context else "",
                            format_fetch_error(e),
                        )
        
        raise DataFetchError(f"数据抓取失败: {format_fetch_error(last_exception)}")


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
                logger.debug("成功导入akshare库")
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

    def _call_with_requests_timeout(self, func, timeout: float = 12, **kwargs):
        """
        部分 akshare 接口没有暴露 timeout 参数；这里只在本次调用期间
        给该函数模块内的 requests.get 加默认 timeout，避免队列线程长时间卡死。
        """
        requests_module = getattr(func, "__globals__", {}).get("requests")
        if requests_module is None or not hasattr(requests_module, "get"):
            return func(**kwargs)

        original_get = requests_module.get

        def get_with_timeout(*args, **request_kwargs):
            request_kwargs.setdefault("timeout", timeout)
            return original_get(*args, **request_kwargs)

        with _REQUESTS_TIMEOUT_PATCH_LOCK:
            requests_module.get = get_with_timeout
            try:
                return func(**kwargs)
            finally:
                requests_module.get = original_get

    def get_all_indices(self) -> List[Dict[str, Any]]:
        """
        获取所有指数列表
        
        Returns:
            List[Dict[str, Any]]: 指数列表，包含指数代码和名称等信息
        """
        if not self.is_available():
            logger.error("akshare不可用，无法获取指数列表")
            return []
        
        def parse_indices(dataframe):
            if dataframe is None or dataframe.empty:
                return []

            indices = []
            seen_codes = set()
            for _, row in dataframe.iterrows():
                code = None
                name = None
                for key in ['代码', '证券代码', '指数代码', 'symbol', 'code', 'index_code']:
                    if key in row and pd.notna(row[key]) and str(row[key]).strip():
                        code = str(row[key]).strip()
                        break
                for key in ['名称', '证券简称', '指数名称', 'name', 'index_name', 'display_name']:
                    if key in row and pd.notna(row[key]) and str(row[key]).strip():
                        name = str(row[key]).strip()
                        break
                if code and code not in seen_codes:
                    seen_codes.add(code)
                    indices.append({'code': code, 'name': name or code})
            return indices

        try:
            logger.info("开始获取所有指数列表")
            try:
                # 聚宽接口目前偶发返回无表格页面，优先保留原始来源。
                df = self._retry_wrapper(self.ak.index_stock_info)
                indices = parse_indices(df)
                if indices:
                    logger.info("成功获取所有指数列表，共%d个指数", len(indices))
                    return indices
            except Exception as primary_error:
                logger.warning("聚宽指数列表接口不可用，准备切换东方财富来源: %s", format_fetch_error(primary_error))

            # 备用来源不依赖 HTML 表格，按指数分类合并并去重。
            fallback_indices = []
            fallback_symbols = ['沪深重要指数', '中证系列指数', '上证系列指数', '深证系列指数']
            for symbol in fallback_symbols:
                try:
                    fallback_df = self._retry_wrapper(
                        lambda **kwargs: self._call_with_requests_timeout(
                            self.ak.stock_zh_index_spot_em,
                            timeout=12,
                            **kwargs,
                        ),
                        symbol=symbol,
                        _retry_context=f"东方财富指数列表 {symbol}",
                    )
                    fallback_indices.extend(parse_indices(fallback_df))
                except Exception as fallback_error:
                    logger.warning("东方财富指数列表分类%s获取失败: %s", symbol, format_fetch_error(fallback_error))

            unique_indices = []
            seen_codes = set()
            for index_info in fallback_indices:
                if index_info['code'] not in seen_codes:
                    seen_codes.add(index_info['code'])
                    unique_indices.append(index_info)

            if unique_indices:
                logger.info("通过东方财富备用来源获取指数列表，共%d个指数", len(unique_indices))
                return unique_indices
            raise DataFetchError("指数列表接口均不可用")
        except Exception as e:
            logger.error(f"获取所有指数列表失败: {format_fetch_error(e)}")
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
     
    def fetch_stock_daily(self, code: str, start_date: str, end_date: str, adjust: str = "", source: str = None) -> pd.DataFrame:
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
            sources = [
                (
                    "stock_zh_a_daily",
                    lambda **kwargs: self._call_with_requests_timeout(
                        self.ak.stock_zh_a_daily,
                        timeout=12,
                        **kwargs,
                    ),
                    {
                        "symbol": make_symbol(code),
                        "start_date": start_date,
                        "end_date": end_date,
                        "adjust": adjust,
                    },
                ),
                (
                    "stock_zh_a_hist",
                    self.ak.stock_zh_a_hist,
                    {
                        "symbol": code,
                        "period": "daily",
                        "start_date": start_date,
                        "end_date": end_date,
                        "adjust": adjust,
                        "timeout": 12,
                    },
                ),
            ]
            if source:
                sources = [item for item in sources if item[0] == source]
                if not sources:
                    raise DataFetchError(f"未知日线数据源: {source}")
            else:
                sources = weighted_source_order(sources)
            source_order = [source_name for source_name, _, _ in sources]
            source_order_log = ",".join(format_source_name(source_name) for source_name in source_order)
            errors = []
            fetch_started_at = datetime.now()
            logger.debug(
                "[日线] start=%s code=%s range=%s~%s adj=%s first=%s order=%s",
                fetch_started_at.strftime("%Y-%m-%d %H:%M:%S"),
                code,
                start_date,
                end_date,
                adjust or "-",
                format_source_name(source_order[0]),
                source_order_log,
            )

            for source_name, source_func, source_kwargs in sources:
                source_started_at = datetime.now()
                try:
                    logger.debug(
                        "尝试使用 %s 获取股票%s数据，时间范围：%s至%s",
                        source_name,
                        code,
                        start_date,
                        end_date,
                    )
                    df = self._retry_wrapper(
                        source_func,
                        **source_kwargs,
                        _retry_context=f"{source_name} code={code} range={start_date}~{end_date} adjust={adjust or '-'}",
                        _log_final=False,
                    )
                    fetch_ended_at = datetime.now()
                    elapsed = (fetch_ended_at - fetch_started_at).total_seconds()
                    rows = 0 if df is None else len(df)
                    logger.info(
                        "[日线] code=%s range=%s~%s first=%s source=%s rows=%d start=%s end=%s cost=%.2fs status=成功",
                        code,
                        start_date,
                        end_date,
                        format_source_name(source_order[0]),
                        format_source_name(source_name),
                        rows,
                        fetch_started_at.strftime("%H:%M:%S"),
                        fetch_ended_at.strftime("%H:%M:%S"),
                        elapsed,
                    )
                    self.last_daily_source = source_name
                    return df
                except Exception as source_error:
                    source_ended_at = datetime.now()
                    errors.append(f"{source_name}: {format_fetch_error(source_error)}")
                    logger.debug(
                        "%s 获取失败，尝试下一个数据源: code=%s range=%s~%s adjust=%s elapsed=%.2fs error=%s",
                        source_name,
                        code,
                        start_date,
                        end_date,
                        adjust or '-',
                        (source_ended_at - source_started_at).total_seconds(),
                        format_fetch_error(source_error),
                    )

            raise DataFetchError("; ".join(errors))
        except Exception as e:
            fetch_ended_at = datetime.now()
            if 'fetch_started_at' in locals():
                logger.warning(
                    "[日线] code=%s range=%s~%s first=%s start=%s end=%s cost=%.2fs status=失败 error=%s",
                    code,
                    start_date,
                    end_date,
                    format_source_name(source_order[0]) if 'source_order' in locals() and source_order else "-",
                    fetch_started_at.strftime("%H:%M:%S"),
                    fetch_ended_at.strftime("%H:%M:%S"),
                    (fetch_ended_at - fetch_started_at).total_seconds(),
                    format_fetch_error(e),
                )
            raise DataFetchError(
                f"获取日K线失败: code={code} range={start_date}~{end_date} adjust={adjust or '-'} error={format_fetch_error(e)}"
            )
    
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
    'get_sw_industry_data',
    'is_trading_day'
]
