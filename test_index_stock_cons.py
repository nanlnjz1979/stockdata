#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试指数成分股获取功能
"""

import logging
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_fetch_index_stock_cons():
    """
    测试获取指数成分股功能
    """
    try:
        from backend.global_config.data_fetch import DataFetchFactory
        
        # 获取AkshareFetcher实例
        fetcher = DataFetchFactory.get_fetcher("akshare")
        
        # 测试获取沪深300指数成分股（默认参数）
        logger.info("测试获取沪深300指数（000300）成分股")
        hs300_stocks = fetcher.fetch_index_stock_cons()
        logger.info(f"成功获取沪深300指数成分股，共{len(hs300_stocks)}只")
        
        # 打印前5只股票作为示例
        if hs300_stocks:
            logger.info("沪深300指数前5只成分股：")
            for stock in hs300_stocks[:5]:
                logger.info(f"代码：{stock['code']}，名称：{stock['name']}")
        
        # 测试获取其他指数成分股
        logger.info("\n测试获取中证500指数（000905）成分股")
        zz500_stocks = fetcher.fetch_index_stock_cons(symbol="000905")
        logger.info(f"成功获取中证500指数成分股，共{len(zz500_stocks)}只")
        
        # 打印前5只股票作为示例
        if zz500_stocks:
            logger.info("中证500指数前5只成分股：")
            for stock in zz500_stocks[:5]:
                logger.info(f"代码：{stock['code']}，名称：{stock['name']}")
        
        return True
        
    except Exception as e:
        logger.error(f"测试获取指数成分股功能时发生错误：{str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_fetch_index_stock_cons()
    
    if success:
        logger.info("\n测试获取指数成分股功能成功！")
    else:
        logger.info("\n测试获取指数成分股功能失败！")
