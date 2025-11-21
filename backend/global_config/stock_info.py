import logging
from typing import List, Dict, Any, Optional
from backend.db.db_pool import get_conn, put_conn


class StockInfo:
    """
    股票信息管理类（无需实例化）
    提供股票基础信息的查询、更新等功能
    使用类方法实现，可直接通过类名调用
    """
    # 类变量，用于存储股票总数，默认为0
    _total_stock_count = 0
    # 类级别的logger
    logger = logging.getLogger(__name__)
    
    @classmethod
    def get_stock_count(cls) -> int:
        """
        获取股票总数，实现懒加载逻辑
        当总数为0时，调用get_all_stocks获取实际总数
        
        Returns:
            int: 股票总数
        """
        try:
            # 如果总数为0，则调用get_all_stocks获取总数
            if cls._total_stock_count == 0:
                cls.logger.info("股票总数为0，开始获取实际总数")
                all_stocks = cls.get_all_stocks()
                cls._total_stock_count = len(all_stocks)
                cls.logger.info(f"成功获取股票总数: {cls._total_stock_count}")
            
            return cls._total_stock_count
        except Exception as e:
            cls.logger.error(f"获取股票总数失败: {str(e)}")
            return 0
    
    @classmethod
    def get_all_stocks(cls) -> List[Dict[str, Any]]:
        """
        获取所有股票基础信息，使用缓存机制提高性能
        
        Returns:
            List[Dict[str, Any]]: 包含所有股票信息的列表
        """
        from backend.global_config.file_config import DataConfig
        from backend.global_config.data_fetch import DataFetchFactory
        
        try:
            # 尝试从缓存中获取股票信息
            list_stocks = DataConfig.get("all_stocks")
            if list_stocks is not None:
                cls.logger.info(f"从缓存中获取股票信息，共{len(list_stocks)}条记录")
                return list_stocks
            
            # 如果缓存不存在，通过DataFetchFactory获取股票信息
            cls.logger.info("缓存中没有股票信息，开始从数据源获取")
            fetcher = DataFetchFactory.get_fetcher("akshare")
            list_stocks = fetcher.fetch_all_stock_basic_info()
            
            cls._total_stock_count = len(list_stocks)
            
            # 将获取的信息存入缓存
            if list_stocks:
                DataConfig.set("all_stocks", list_stocks)
                cls.logger.info(f"成功获取并缓存股票信息，共{len(list_stocks)}条记录")
            else:
                cls.logger.warning("未获取到股票信息")
                
            return list_stocks
        except Exception as e:
            cls.logger.error(f"获取所有股票信息失败: {str(e)}")
            return []
    
    
    
    
    
    @classmethod
    def get_company_name_by_code(cls, code: str) -> Optional[str]:
        """
        根据股票代码获取对应的公司名称
        
        Args:
            code: 股票代码
            
        Returns:
            Optional[str]: 公司名称，如果股票不存在则返回None
        """
        try:
            cls.logger.info(f"开始获取股票代码{code}对应的公司名称")
            
            # 从缓存中获取所有股票信息
            all_stocks = cls.get_all_stocks()
            
            # 查找对应的股票信息
            for stock in all_stocks:
                if stock.get('code') == code:
                    company_name = stock.get('company_name')
                    cls.logger.info(f"成功获取股票代码{code}的公司名称: {company_name}")
                    return company_name
            
            cls.logger.warning(f"未找到股票代码{code}对应的信息")
            return None
        except Exception as e:
            cls.logger.error(f"获取股票代码{code}的公司名称失败: {str(e)}")
            return None
    
    
    @classmethod
    def is_stock_exist(cls, code: str) -> bool:
        """
        检查股票是否存在
        
        Args:
            code: 股票代码
            
        Returns:
            bool: 股票是否存在
        """
        company_name = cls.get_company_name_by_code(code)
        return company_name is not None
    
    @classmethod
    def get_stocks_by_market(cls, market: str) -> List[Dict[str, Any]]:
        """
        根据市场代码获取对应市场的股票信息列表
        
        Args:
            market: 市场代码，如'SH'（上海）、'SZ'（深圳）、'BJ'（北京）
            
        Returns:
            List[Dict[str, Any]]: 指定市场的股票信息列表，发生错误时返回空列表
        """
        try:
            cls.logger.info(f"开始获取市场{market}的股票信息列表")
            
            # 验证市场代码格式
            if not market or not isinstance(market, str):
                cls.logger.warning(f"无效的市场代码: {market}，应为非空字符串")
                return []
            
            # 转换为大写，确保格式统一
            market = market.upper()
            
            # 验证市场代码的有效性
            valid_markets = ['SH', 'SZ', 'BJ']
            if market not in valid_markets:
                cls.logger.warning(f"不支持的市场代码: {market}，支持的市场: {valid_markets}")
                return []
            
            # 从缓存获取所有股票信息
            all_stocks = cls.get_all_stocks()
            
            # 根据market过滤股票信息
            # 这里假设股票代码的前2个字符表示市场，例如'SH'开头表示上海市场
            filtered_stocks = []
            for stock in all_stocks:
                # 检查股票代码是否以市场代码开头
                if stock.get('code', '').startswith(market):
                    filtered_stocks.append(stock)
                # 也可以检查stock.get('market') == market，根据实际数据结构调整
            
            cls.logger.info(f"成功获取市场{market}的股票信息，共{len(filtered_stocks)}条记录")
            return filtered_stocks
            
        except Exception as e:
            cls.logger.error(f"获取市场{market}的股票信息失败: {str(e)}")
            return []